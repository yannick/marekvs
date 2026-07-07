//! Property tests for the proto field-record CRDT (design/18): the final
//! record set is independent of merge order, and concurrent repeated-field
//! runs never interleave. Mirrors json_laws.rs — proto field records reuse
//! the same RecordType machinery (HashField ORSWOT / List LWW), so these
//! laws fold records through the production `merge_values`.

use marekvs_core::envelope::{Envelope, RecordType};
use marekvs_core::json::{rga_order, Eid, EID_HEAD};
use marekvs_core::merge::{element_remove, element_set, merge_values, resolve, Dot};
use marekvs_core::pdoc::{decode_path, encode_path, MKey, PArrElem, PSeg, PVal};
use proptest::prelude::*;
use std::collections::HashMap;

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
}

fn node_record(hlc: u64, origin: u16, val: &PVal, observed: &[Dot]) -> Vec<u8> {
    element_set(RecordType::HashField, hlc, origin, &val.encode(), observed)
}

fn elem_record(hlc: u64, origin: u16, elem: &PArrElem, live: bool) -> Vec<u8> {
    let env = if live {
        Envelope::new(RecordType::List, hlc, origin)
    } else {
        Envelope::tombstone(RecordType::List, hlc, origin)
    };
    env.encode_with(&elem.encode())
}

fn fold(records: &[(Vec<u8>, Vec<u8>)], order: &[usize]) -> PathStore {
    let mut store = PathStore::default();
    for &i in order {
        let (p, r) = &records[i];
        store.apply(p, r);
    }
    store
}

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

/// One generated op over a small fixed universe of field paths.
#[derive(Debug, Clone)]
enum Op {
    /// Set a singular field (covers the dots this op observed at gen time).
    SetField { field: u8, scalar: i32, origin: u16 },
    /// Observed-remove a singular field.
    DelField { field: u8, origin: u16 },
    /// Set a map entry under field 14.
    SetMapKey { key: u8, scalar: i32, origin: u16 },
    /// Append to the repeated field 15.
    Append { scalar: i32, origin: u16 },
    /// Tombstone a previously appended element.
    PopLast { origin: u16 },
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0u8..3, -50i32..50, 0u16..4).prop_map(|(field, scalar, origin)| Op::SetField {
            field,
            scalar,
            origin
        }),
        (0u8..3, 0u16..4).prop_map(|(field, origin)| Op::DelField { field, origin }),
        (0u8..3, -50i32..50, 0u16..4).prop_map(|(key, scalar, origin)| Op::SetMapKey {
            key,
            scalar,
            origin
        }),
        (-50i32..50, 0u16..4).prop_map(|(scalar, origin)| Op::Append { scalar, origin }),
        (0u16..4).prop_map(|origin| Op::PopLast { origin }),
    ]
}

fn field_path(n: u32) -> Vec<u8> {
    encode_path(&[PSeg::Field(n)])
}

/// Run ops as a sequential history against a generator store (each op
/// observes everything before it), collecting every emitted record.
fn records_of(ops: &[Op]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut gen_store = PathStore::default();
    let mut records: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut hlc = 1000u64;
    let mut appended: Vec<Eid> = Vec::new();
    let mut alive: Vec<Eid> = Vec::new();

    let observed = |store: &PathStore, path: &[u8]| -> Vec<Dot> {
        store
            .recs
            .get(path)
            .and_then(|r| {
                Envelope::decode(r).map(|(_, pay)| marekvs_core::merge::element_dots(pay))
            })
            .unwrap_or_default()
    };

    let emit =
        |store: &mut PathStore, recs: &mut Vec<(Vec<u8>, Vec<u8>)>, path: Vec<u8>, rec: Vec<u8>| {
            store.apply(&path, &rec);
            recs.push((path, rec));
        };

    // pre-seed: root Msg, map marker at 14, list marker at 15
    emit(
        &mut gen_store,
        &mut records,
        vec![],
        node_record(hlc, 0, &PVal::Msg, &[]),
    );
    hlc += 1;
    emit(
        &mut gen_store,
        &mut records,
        field_path(14),
        node_record(hlc, 0, &PVal::Map, &[]),
    );
    hlc += 1;
    emit(
        &mut gen_store,
        &mut records,
        field_path(15),
        node_record(hlc, 0, &PVal::List, &[]),
    );

    for op in ops {
        hlc += 1;
        match op {
            Op::SetField {
                field,
                scalar,
                origin,
            } => {
                let path = field_path(1 + *field as u32);
                let obs = observed(&gen_store, &path);
                let rec = node_record(hlc, *origin, &PVal::I32(*scalar), &obs);
                emit(&mut gen_store, &mut records, path, rec);
            }
            Op::DelField { field, origin } => {
                let path = field_path(1 + *field as u32);
                let obs = observed(&gen_store, &path);
                if obs.is_empty() {
                    continue;
                }
                let rec = element_remove(RecordType::HashField, hlc, *origin, &obs);
                emit(&mut gen_store, &mut records, path, rec);
            }
            Op::SetMapKey {
                key,
                scalar,
                origin,
            } => {
                let path = encode_path(&[PSeg::Field(14), PSeg::MapKey(MKey::U32(*key as u32))]);
                let obs = observed(&gen_store, &path);
                let rec = node_record(hlc, *origin, &PVal::I32(*scalar), &obs);
                emit(&mut gen_store, &mut records, path, rec);
            }
            Op::Append { scalar, origin } => {
                let e = Eid {
                    hlc,
                    origin: *origin,
                };
                let left = *appended.last().unwrap_or(&EID_HEAD);
                let elem = PArrElem {
                    left,
                    val: PVal::I32(*scalar),
                };
                let path = encode_path(&[PSeg::Field(15), PSeg::Elem(e)]);
                let rec = elem_record(hlc, *origin, &elem, true);
                emit(&mut gen_store, &mut records, path, rec);
                appended.push(e);
                alive.push(e);
            }
            Op::PopLast { origin } => {
                let Some(e) = alive.pop() else { continue };
                let path = encode_path(&[PSeg::Field(15), PSeg::Elem(e)]);
                // tombstone preserves the stored payload (RGA anchor)
                let stored = gen_store.recs.get(&path).unwrap();
                let (_, pay) = Envelope::decode(stored).unwrap();
                let elem = PArrElem::decode(pay).unwrap();
                let rec = elem_record(hlc, *origin, &elem, false);
                emit(&mut gen_store, &mut records, path, rec);
            }
        }
    }
    records
}

/// Extract the repeated field's materialized element order from a store.
fn list_order(store: &PathStore) -> Vec<Eid> {
    let mut elems: Vec<(Eid, Eid, bool)> = Vec::new();
    let list_prefix = field_path(15);
    for (path, rec) in &store.recs {
        if !path.starts_with(&list_prefix) || path == &list_prefix {
            continue;
        }
        let Some(PSeg::Elem(e)) = decode_path(path).unwrap().pop() else {
            continue;
        };
        let (env, pay) = Envelope::decode(rec).unwrap();
        let elem = PArrElem::decode(pay).unwrap();
        elems.push((e, elem.left, !env.is_tombstone()));
    }
    rga_order(&elems)
        .into_iter()
        .filter_map(|(e, live)| live.then_some(e))
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// Any merge order of the record stream converges to the identical
    /// record set AND the identical materialized repeated-field order.
    #[test]
    fn record_set_permutation_independent(ops in prop::collection::vec(op_strategy(), 1..12)) {
        let records = records_of(&ops);
        let baseline = assert_permutation_independent(&records);
        let base_order = list_order(&baseline);
        let n = records.len();
        for rot in 1..n {
            let mut order: Vec<usize> = (0..n).collect();
            order.rotate_left(rot);
            prop_assert_eq!(&list_order(&fold(&records, &order)), &base_order);
        }
    }

    /// Two concurrent append runs on the same repeated field stay contiguous
    /// after merge (RGA non-interleaving), in both delivery orders.
    #[test]
    fn concurrent_runs_stay_contiguous(len_a in 1usize..5, len_b in 1usize..5) {
        let mut records = Vec::new();
        let mk_run = |records: &mut Vec<(Vec<u8>, Vec<u8>)>, base_hlc: u64, origin: u16, len: usize| {
            let mut run = Vec::new();
            let mut left = EID_HEAD;
            for i in 0..len {
                let e = Eid { hlc: base_hlc + i as u64, origin };
                let elem = PArrElem { left, val: PVal::I32(i as i32) };
                records.push((
                    encode_path(&[PSeg::Field(15), PSeg::Elem(e)]),
                    elem_record(e.hlc, origin, &elem, true),
                ));
                run.push(e);
                left = e;
            }
            run
        };
        records.push((field_path(15), node_record(1, 0, &PVal::List, &[])));
        let run_a = mk_run(&mut records, 100, 1, len_a);
        let run_b = mk_run(&mut records, 200, 2, len_b);
        let store = assert_permutation_independent(&records);
        let order = list_order(&store);
        let mut expected = run_b.clone(); // higher hlc run sorts first
        expected.extend_from_slice(&run_a);
        prop_assert_eq!(order, expected);
    }
}
