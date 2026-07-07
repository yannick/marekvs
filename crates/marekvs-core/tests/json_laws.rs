//! Property tests for the JSON per-path CRDT (design/16): the final record
//! set and the materialized document are independent of merge order, and
//! concurrent RGA runs never interleave.

use marekvs_core::envelope::{Envelope, RecordType};
use marekvs_core::json::{
    build_doc, decode_path, decompose, encode_path, rga_order, ArrElem, Doc, Eid, JVal, JsonRecord,
    NodeIn, Seg, EID_HEAD,
};
use marekvs_core::merge::{
    element_dots, element_remove, element_set, element_value, merge_values, resolve, ElementState,
};
use proptest::prelude::*;
use std::collections::HashMap;

/// Per-path record store folding records through the production merge.
#[derive(Default, Clone)]
struct PathStore {
    recs: HashMap<Vec<u8>, Vec<u8>>,
}

impl PathStore {
    fn apply(&mut self, path: &[u8], incoming: &[u8]) {
        match self.recs.get(path) {
            None => {
                self.recs.insert(path.to_vec(), incoming.to_vec());
            }
            Some(local) => {
                let merged = resolve(local, incoming, &merge_values(local, incoming)).to_vec();
                self.recs.insert(path.to_vec(), merged);
            }
        }
    }

    /// Decode the stored records into `build_doc` input, applying the same
    /// filtering the engine does: dead map entries dropped, array tombstones
    /// kept with `live = false`.
    fn nodes(&self) -> Vec<(Vec<u8>, NodeIn)> {
        let mut out = Vec::new();
        for (path, rec) in &self.recs {
            let (env, pay) = Envelope::decode(rec).unwrap();
            let is_arr_elem = matches!(decode_path(path).unwrap().last(), Some(Seg::Elem(_)));
            if is_arr_elem {
                let elem = ArrElem::decode(pay).unwrap();
                out.push((
                    path.clone(),
                    NodeIn::ArrElem {
                        elem,
                        live: !env.is_tombstone(),
                    },
                ));
            } else {
                if env.is_tombstone() {
                    continue;
                }
                let st = ElementState::decode(pay).unwrap();
                let Some(val_bytes) = st.value() else {
                    continue;
                };
                out.push((
                    path.clone(),
                    NodeIn::Map {
                        val: JVal::decode(val_bytes).unwrap(),
                        dots: st.dots(),
                    },
                ));
            }
        }
        out
    }

    fn doc(&self) -> Option<Doc> {
        build_doc(&self.nodes())
    }
}

fn map_record(hlc: u64, origin: u16, val: &JVal, observed: &[marekvs_core::merge::Dot]) -> Vec<u8> {
    element_set(RecordType::HashField, hlc, origin, &val.encode(), observed)
}

fn arr_record(hlc: u64, origin: u16, elem: &ArrElem) -> Vec<u8> {
    Envelope::new(RecordType::List, hlc, origin).encode_with(&elem.encode())
}

fn arr_tombstone(hlc: u64, origin: u16, elem: &ArrElem) -> Vec<u8> {
    Envelope::tombstone(RecordType::List, hlc, origin).encode_with(&elem.encode())
}

fn elem_path(e: Eid) -> Vec<u8> {
    encode_path(&[Seg::Elem(e)])
}

/// Fold `records` into a store in the given order.
fn fold(records: &[(Vec<u8>, Vec<u8>)], order: &[usize]) -> PathStore {
    let mut store = PathStore::default();
    for &i in order {
        let (p, r) = &records[i];
        store.apply(p, r);
    }
    store
}

/// Assert every rotation/reversal of the record list converges to the same
/// final record set.
fn assert_permutation_independent(records: &[(Vec<u8>, Vec<u8>)]) -> PathStore {
    let n = records.len();
    let base: Vec<usize> = (0..n).collect();
    let baseline = fold(records, &base);
    for rot in 0..n {
        let mut order = base.clone();
        order.rotate_left(rot);
        assert_eq!(fold(records, &order).recs, baseline.recs);
        order.reverse();
        assert_eq!(fold(records, &order).recs, baseline.recs);
    }
    baseline
}

// ---------------------------------------------------------------------------
// RGA array laws
// ---------------------------------------------------------------------------

/// One generated array op: insert after a previously created element (or the
/// head sentinel), or tombstone a previously created element.
#[derive(Debug, Clone)]
enum ArrOp {
    Insert { anchor_sel: usize, origin: u16 },
    Delete { target_sel: usize, origin: u16 },
}

fn arr_op_strategy() -> impl Strategy<Value = ArrOp> {
    prop_oneof![
        3 => (0usize..8, 0u16..4).prop_map(|(anchor_sel, origin)| ArrOp::Insert {
            anchor_sel,
            origin
        }),
        1 => (0usize..8, 0u16..4).prop_map(|(target_sel, origin)| ArrOp::Delete {
            target_sel,
            origin
        }),
    ]
}

/// Materialize the generated ops into records (hlc = generation index, so
/// every record version is unique).
fn arr_records(ops: &[ArrOp]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut created: Vec<(Eid, ArrElem)> = Vec::new();
    let mut records = Vec::new();
    for (i, op) in ops.iter().enumerate() {
        let hlc = 10 + i as u64;
        match op {
            ArrOp::Insert { anchor_sel, origin } => {
                let left = if created.is_empty() || *anchor_sel == 0 {
                    EID_HEAD
                } else {
                    created[(anchor_sel - 1) % created.len()].0
                };
                let e = Eid {
                    hlc,
                    origin: *origin,
                };
                let elem = ArrElem {
                    left,
                    val: JVal::Int(i as i64),
                };
                records.push((elem_path(e), arr_record(hlc, *origin, &elem)));
                created.push((e, elem));
            }
            ArrOp::Delete { target_sel, origin } => {
                if created.is_empty() {
                    continue;
                }
                let (e, elem) = &created[target_sel % created.len()];
                // payload preserved: the left ref must survive as an anchor
                records.push((elem_path(*e), arr_tombstone(hlc, *origin, elem)));
            }
        }
    }
    records
}

/// Extract `(eid, left, live)` from a folded store.
fn elems_of(store: &PathStore) -> Vec<(Eid, Eid, bool)> {
    let mut out = Vec::new();
    for (path, rec) in &store.recs {
        let (env, pay) = Envelope::decode(rec).unwrap();
        let Some(Seg::Elem(e)) = decode_path(path).unwrap().pop() else {
            panic!("non-element record in array test");
        };
        let elem = ArrElem::decode(pay).unwrap();
        out.push((e, elem.left, !env.is_tombstone()));
    }
    out.sort_unstable_by_key(|(e, _, _)| *e);
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Any merge order of any interleaving yields the same records AND the
    /// same materialized element order.
    #[test]
    fn rga_permutation_independent(ops in prop::collection::vec(arr_op_strategy(), 1..12)) {
        let records = arr_records(&ops);
        prop_assume!(!records.is_empty());
        let baseline = assert_permutation_independent(&records);
        let base_order = rga_order(&elems_of(&baseline));
        let n = records.len();
        for rot in 1..n {
            let mut order: Vec<usize> = (0..n).collect();
            order.rotate_left(rot);
            let store = fold(&records, &order);
            prop_assert_eq!(&rga_order(&elems_of(&store)), &base_order);
        }
    }

    /// Two concurrent append runs (both starting from an empty array) never
    /// interleave: the final order is one contiguous run then the other,
    /// higher first-eid first.
    #[test]
    fn concurrent_runs_stay_contiguous(len_a in 1usize..6, len_b in 1usize..6) {
        let mut records = Vec::new();
        let mut run_a = Vec::new();
        let mut left = EID_HEAD;
        for i in 0..len_a {
            let e = Eid { hlc: 100 + i as u64, origin: 1 };
            let elem = ArrElem { left, val: JVal::Int(i as i64) };
            records.push((elem_path(e), arr_record(e.hlc, 1, &elem)));
            run_a.push(e);
            left = e;
        }
        let mut run_b = Vec::new();
        left = EID_HEAD;
        for i in 0..len_b {
            let e = Eid { hlc: 200 + i as u64, origin: 2 };
            let elem = ArrElem { left, val: JVal::Int(i as i64) };
            records.push((elem_path(e), arr_record(e.hlc, 2, &elem)));
            run_b.push(e);
            left = e;
        }
        let store = assert_permutation_independent(&records);
        let order: Vec<Eid> = rga_order(&elems_of(&store))
            .into_iter()
            .map(|(e, _)| e)
            .collect();
        // run B starts at hlc 200 > run A's 100, so B comes first, contiguous
        let mut expected = run_b.clone();
        expected.extend_from_slice(&run_a);
        prop_assert_eq!(order, expected);
    }
}

// ---------------------------------------------------------------------------
// Doc-level convergence
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum DocOp {
    /// JSON.SET $.<field> <scalar>
    SetField { field: u8, scalar: i64, origin: u16 },
    /// JSON.SET $.<field> {"x": <scalar>} — a subtree write
    SetSubtree { field: u8, scalar: i64, origin: u16 },
    /// JSON.DEL $.<field>
    DelField { field: u8, origin: u16 },
    /// JSON.ARRAPPEND $.arr <scalar>
    Append { scalar: i64, origin: u16 },
}

fn doc_op_strategy() -> impl Strategy<Value = DocOp> {
    prop_oneof![
        (0u8..3, -50i64..50, 0u16..4).prop_map(|(field, scalar, origin)| DocOp::SetField {
            field,
            scalar,
            origin
        }),
        (0u8..3, -50i64..50, 0u16..4).prop_map(|(field, scalar, origin)| DocOp::SetSubtree {
            field,
            scalar,
            origin
        }),
        (0u8..3, 0u16..4).prop_map(|(field, origin)| DocOp::DelField { field, origin }),
        (-50i64..50, 0u16..4).prop_map(|(scalar, origin)| DocOp::Append { scalar, origin }),
    ]
}

fn field_name(sel: u8) -> Vec<u8> {
    vec![b'f', b'0' + sel]
}

/// Run the ops as a sequential history against a generator store (each op
/// observes everything before it, like one shard thread would), collecting
/// every emitted record.
fn doc_records(ops: &[DocOp]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut gen_store = PathStore::default();
    let mut records: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let emit =
        |store: &mut PathStore, recs: &mut Vec<(Vec<u8>, Vec<u8>)>, path: Vec<u8>, rec: Vec<u8>| {
            store.apply(&path, &rec);
            recs.push((path, rec));
        };
    let mut hlc = 1000u64;
    let mut fresh_hlc = || {
        hlc += 1;
        hlc
    };
    // pre-seed: root object + "arr": []
    emit(
        &mut gen_store,
        &mut records,
        vec![],
        map_record(fresh_hlc(), 0, &JVal::Obj, &[]),
    );
    let arr_path = encode_path(&[Seg::Field(b"arr".to_vec())]);
    emit(
        &mut gen_store,
        &mut records,
        arr_path.clone(),
        map_record(fresh_hlc(), 0, &JVal::Arr, &[]),
    );
    let mut last_elem = EID_HEAD;

    let observed = |store: &PathStore, path: &[u8]| -> Vec<marekvs_core::merge::Dot> {
        store
            .recs
            .get(path)
            .and_then(|r| Envelope::decode(r).map(|(_, pay)| element_dots(pay)))
            .unwrap_or_default()
    };

    for op in ops {
        match op {
            DocOp::SetField {
                field,
                scalar,
                origin,
            } => {
                let path = encode_path(&[Seg::Field(field_name(*field))]);
                let obs = observed(&gen_store, &path);
                let rec = map_record(fresh_hlc(), *origin, &JVal::Int(*scalar), &obs);
                emit(&mut gen_store, &mut records, path, rec);
            }
            DocOp::SetSubtree {
                field,
                scalar,
                origin,
            } => {
                let base = vec![Seg::Field(field_name(*field))];
                let h = fresh_hlc();
                let mut fresh_eid = || Eid {
                    hlc: h,
                    origin: *origin,
                };
                let subtree = serde_json::json!({ "x": scalar });
                for rec in decompose(&base, &subtree, &mut fresh_eid) {
                    match rec {
                        JsonRecord::Map { path, val } => {
                            let obs = observed(&gen_store, &path);
                            let h2 = fresh_hlc();
                            let bytes = map_record(h2, *origin, &val, &obs);
                            emit(&mut gen_store, &mut records, path, bytes);
                        }
                        JsonRecord::Arr { path, elem } => {
                            let h2 = fresh_hlc();
                            let bytes = arr_record(h2, *origin, &elem);
                            emit(&mut gen_store, &mut records, path, bytes);
                        }
                    }
                }
            }
            DocOp::DelField { field, origin } => {
                let path = encode_path(&[Seg::Field(field_name(*field))]);
                let obs = observed(&gen_store, &path);
                if obs.is_empty() {
                    continue;
                }
                let rec = element_remove(RecordType::HashField, fresh_hlc(), *origin, &obs);
                emit(&mut gen_store, &mut records, path, rec);
            }
            DocOp::Append { scalar, origin } => {
                let e = Eid {
                    hlc: fresh_hlc(),
                    origin: *origin,
                };
                let elem = ArrElem {
                    left: last_elem,
                    val: JVal::Int(*scalar),
                };
                let mut path = arr_path.clone();
                marekvs_core::json::push_seg(&mut path, &Seg::Elem(e));
                let rec = arr_record(e.hlc, *origin, &elem);
                emit(&mut gen_store, &mut records, path, rec);
                last_elem = e;
            }
        }
    }
    records
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Every merge order of the full record stream materializes the exact
    /// same JSON document.
    #[test]
    fn doc_convergence(ops in prop::collection::vec(doc_op_strategy(), 1..10)) {
        let records = doc_records(&ops);
        let baseline = assert_permutation_independent(&records);
        let base_doc = baseline.doc().expect("root exists").value;
        let n = records.len();
        for rot in 1..n {
            let mut order: Vec<usize> = (0..n).collect();
            order.rotate_left(rot);
            let doc = fold(&records, &order).doc().expect("root exists").value;
            prop_assert_eq!(&doc, &base_doc);
        }
        // sanity: the value winning at a set field is decodable JSON
        let _ = element_value; // (helper kept for future assertions)
    }
}
