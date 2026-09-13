//! Identity-preserving uploads compiled into ordinary per-path CRDT deltas.
use marekvs_core::json::{self, ArrElem, Eid, JsonRecord, NodeIn, Seg};
use marekvs_diff::{
    graph::{Graph, NodeRef, Op},
    matching::Matching,
    Budget, CancelToken, DiffError, Tree,
};
use std::collections::{BTreeMap, BTreeSet};

use super::{
    compare::{self, StoredGraph},
    keys::DiffKey,
    pool::DiffConfig,
    snapshot,
};
use crate::cmd::json::doc::{remove_map_node, tomb_arr_node, write_arr_node, write_map_node};
use crate::{
    reply::Reply,
    store::{self, ShardCtx},
    Engine,
};
use marekvs_core::{
    envelope::{head, Envelope},
    ikey,
    json::JVal,
    merge::Dot,
};

use std::sync::Arc;

/// Same wire layout as core decomposition, with checks before every allocation.
fn decompose_bounded(
    value: &serde_json::Value,
    choose: &mut dyn FnMut(&[u8], usize) -> Eid,
    budget: &Budget,
    cancel: &CancelToken,
) -> Result<Vec<JsonRecord>, DiffError> {
    fn push(
        out: &mut Vec<JsonRecord>,
        record: JsonRecord,
        bytes: &mut usize,
        budget: &Budget,
        cancel: &CancelToken,
    ) -> Result<(), DiffError> {
        cancel.check()?;
        let payload = match &record {
            JsonRecord::Map { val, .. } => val.encode().len(),
            JsonRecord::Arr { elem, .. } => elem.encode().len(),
        };
        *bytes = bytes
            .saturating_add(record.path().len())
            .saturating_add(payload)
            .saturating_add(64);
        if out.len() >= budget.max_nodes.saturating_mul(64) || *bytes > budget.max_bytes {
            return Err(DiffError::TooBig {
                bound: "import_record_bytes",
                n: *bytes,
                limit: budget.max_bytes,
            });
        }
        out.push(record);
        Ok(())
    }
    fn children(
        path: &[u8],
        v: &serde_json::Value,
        out: &mut Vec<JsonRecord>,
        bytes: &mut usize,
        choose: &mut dyn FnMut(&[u8], usize) -> Eid,
        budget: &Budget,
        cancel: &CancelToken,
    ) -> Result<(), DiffError> {
        cancel.check()?;
        match v {
            serde_json::Value::Object(map) => {
                for (key, v) in map {
                    let mut p = path.to_vec();
                    json::push_seg(&mut p, &Seg::Field(key.as_bytes().to_vec()));
                    push(
                        out,
                        JsonRecord::Map {
                            path: p.clone(),
                            val: json::jval_of(v),
                        },
                        bytes,
                        budget,
                        cancel,
                    )?;
                    children(&p, v, out, bytes, choose, budget, cancel)?;
                }
            }
            serde_json::Value::Array(array) => {
                let mut left = json::EID_HEAD;
                for (i, v) in array.iter().enumerate() {
                    cancel.check()?;
                    let eid = choose(path, i);
                    let mut p = path.to_vec();
                    json::push_seg(&mut p, &Seg::Elem(eid));
                    push(
                        out,
                        JsonRecord::Arr {
                            path: p.clone(),
                            elem: ArrElem {
                                left,
                                val: json::jval_of(v),
                            },
                        },
                        bytes,
                        budget,
                        cancel,
                    )?;
                    children(&p, v, out, bytes, choose, budget, cancel)?;
                    left = eid;
                }
            }
            _ => {}
        }
        Ok(())
    }
    let mut out = Vec::new();
    let mut bytes = 0;
    push(
        &mut out,
        JsonRecord::Map {
            path: vec![],
            val: json::jval_of(value),
        },
        &mut bytes,
        budget,
        cancel,
    )?;
    children(&[], value, &mut out, &mut bytes, choose, budget, cancel)?;
    Ok(out)
}

/// Generate the complete desired record set while retaining stable source
/// identities. A moved container gets a new prefix, so decomposition naturally
/// transcribes every descendant instead of leaving a marker-only subtree.
fn desired_records(
    a: &Tree,
    b: &Tree,
    comparison: (&Matching, &Graph),
    source: &[(Vec<u8>, NodeIn)],
    fresh: &mut dyn FnMut() -> Eid,
    budget: &Budget,
    cancel: &CancelToken,
) -> Result<Vec<JsonRecord>, DiffError> {
    let (m, g) = comparison;
    cancel.check()?;
    if a.sid == b.sid && !source.is_empty() {
        return Ok(source
            .iter()
            .filter_map(|(path, node)| match node {
                NodeIn::Map { val, .. } => Some(JsonRecord::Map {
                    path: path.clone(),
                    val: val.clone(),
                }),
                NodeIn::ArrElem { elem, live: true } => Some(JsonRecord::Arr {
                    path: path.clone(),
                    elem: elem.clone(),
                }),
                _ => None,
            })
            .collect());
    }
    let doc = json::build_doc(source);
    let index = doc.as_ref().map(|d| &d.index);
    let old: BTreeMap<_, _> = source.iter().map(|(p, n)| (p.clone(), n)).collect();
    let mut paths = vec![Vec::new(); a.nodes.len()];
    for (ai, n) in a.nodes.iter().enumerate() {
        let mut array = paths[ai].clone();
        json::push_seg(&mut array, &Seg::Field(b"c".to_vec()));
        if let Some(info) = index.and_then(|idx| idx.arrays.get(&array)) {
            for (&child, &eid) in n.children.iter().zip(&info.order) {
                let mut path = array.clone();
                json::push_seg(&mut path, &Seg::Elem(eid));
                paths[child as usize] = path;
            }
        }
    }
    let moved: BTreeSet<_> = g
        .changes
        .iter()
        .filter_map(|c| match c.op {
            Op::Move {
                node: NodeRef::A(lid),
                ..
            } => Some(lid),
            _ => None,
        })
        .collect();
    let mut arrays = BTreeMap::new();
    arrays.insert(json::encode_path(&[Seg::Field(b"c".to_vec())]), b.root);
    let mut retained_left = BTreeMap::new();
    let mut counter = 0usize;
    let mut records = decompose_bounded(
        &b.to_json(),
        &mut |array, ordinal| {
            counter = counter.saturating_add(1);
            let structural = arrays
                .get(array)
                .copied()
                .and_then(|parent| b.nodes[parent as usize].children.get(ordinal).copied());
            let candidate = if let Some(bi) = structural {
                m.b2a(bi)
                    .filter(|&ai| !moved.contains(&a.nodes[ai as usize].lid))
                    .and_then(|ai| {
                        let path = &paths[ai as usize];
                        match json::split_last(path) {
                            Some((parent, Seg::Elem(eid))) if parent == array => {
                                Some((path.clone(), eid))
                            }
                            _ => None,
                        }
                    })
            } else {
                index
                    .and_then(|idx| idx.arrays.get(array))
                    .and_then(|info| info.order.get(ordinal))
                    .map(|&eid| {
                        let mut path = array.to_vec();
                        json::push_seg(&mut path, &Seg::Elem(eid));
                        (path, eid)
                    })
            };
            let eid = if let Some((path, eid)) = candidate {
                if let Some(NodeIn::ArrElem { elem, live: true }) = old.get(&path) {
                    retained_left.insert(path, elem.left);
                    eid
                } else {
                    fresh()
                }
            } else {
                fresh()
            };
            if let Some(bi) = structural {
                let mut child_array = array.to_vec();
                json::push_seg(&mut child_array, &Seg::Elem(eid));
                json::push_seg(&mut child_array, &Seg::Field(b"c".to_vec()));
                arrays.insert(child_array, bi);
            }
            eid
        },
        budget,
        cancel,
    )?;
    cancel.check()?;
    // Canonical input bytes bound this transient record decomposition; impose
    // an explicit record multiplier as fields and run tuples are separate nodes.
    if counter > budget.max_nodes.saturating_mul(16)
        || records.len() > budget.max_nodes.saturating_mul(64)
    {
        return Err(DiffError::TooBig {
            bound: "import_records",
            n: records.len(),
            limit: budget.max_nodes.saturating_mul(64),
        });
    }
    for rec in &mut records {
        if let JsonRecord::Arr { path, elem } = rec {
            if let Some(left) = retained_left.get(path) {
                elem.left = *left;
            }
        }
    }
    Ok(records)
}

#[derive(Debug)]
enum Delta {
    Map {
        path: Vec<u8>,
        val: JVal,
        observed: Vec<Dot>,
    },
    Remove {
        path: Vec<u8>,
        observed: Vec<Dot>,
    },
    Array {
        path: Vec<u8>,
        elem: ArrElem,
        live: bool,
    },
}
fn deltas(
    source: &[(Vec<u8>, NodeIn)],
    destination: &[(Vec<u8>, NodeIn)],
    desired: Vec<JsonRecord>,
    cancel: &CancelToken,
) -> Result<Vec<Delta>, DiffError> {
    let mut out = Vec::new();
    let old: BTreeMap<_, _> = destination.iter().map(|(p, n)| (p.clone(), n)).collect();
    let wanted: BTreeMap<_, _> = desired
        .into_iter()
        .map(|r| (r.path().to_vec(), r))
        .collect();
    for (path, rec) in &wanted {
        cancel.check()?;
        match rec {
            JsonRecord::Map { val, .. } => {
                if matches!(old.get(path),Some(NodeIn::Map{val:v,..}) if v==val) {
                    continue;
                }
                let observed = match old.get(path) {
                    Some(NodeIn::Map { dots, .. }) => dots.clone(),
                    _ => vec![],
                };
                out.push(Delta::Map {
                    path: path.clone(),
                    val: val.clone(),
                    observed,
                });
            }
            JsonRecord::Arr { elem, .. } => {
                if matches!(old.get(path),Some(NodeIn::ArrElem{elem:e,live:true}) if e==elem) {
                    continue;
                }
                out.push(Delta::Array {
                    path: path.clone(),
                    elem: elem.clone(),
                    live: true,
                });
            }
        }
    }
    // Preserve source anchors even on a fresh FROM destination. A surviving
    // record may still point to a tombstoned predecessor instead of the nearest
    // currently live predecessor; dropping that anchor would orphan it.
    let mut dead: BTreeMap<Vec<u8>, ArrElem> = source
        .iter()
        .filter_map(|(p, n)| match n {
            NodeIn::ArrElem { elem, .. } if !wanted.contains_key(p) => {
                Some((p.clone(), elem.clone()))
            }
            _ => None,
        })
        .collect();
    for (path, node) in destination {
        cancel.check()?;
        if wanted.contains_key(path) {
            continue;
        }
        match node {
            NodeIn::Map { dots, .. } => out.push(Delta::Remove {
                path: path.clone(),
                observed: dots.clone(),
            }),
            NodeIn::ArrElem { elem, live: true } => {
                dead.entry(path.clone()).or_insert_with(|| elem.clone());
            }
            _ => {}
        }
    }
    for (path, elem) in dead {
        if matches!(old.get(&path),Some(NodeIn::ArrElem{elem:e,live:false}) if e==&elem) {
            continue;
        }
        out.push(Delta::Array {
            path,
            elem,
            live: false,
        });
    }
    Ok(out)
}
fn write_delta(ctx: &ShardCtx, key: &[u8], delta: &[Delta], initialize: bool) {
    if delta.is_empty() {
        return;
    }
    let previous = store::get_head(ctx, key);
    let now = store::now_ms();
    let mut del = previous.map_or(0, |(env, _, d)| {
        d.max(if env.is_tombstone() { env.hlc } else { 0 })
            .max(if env.is_expired(now) {
                env.expiry_hlc()
            } else {
                0
            })
    });
    if initialize {
        del = del.max(crate::cmd::generic::del_key_hlc(ctx, key).unwrap_or(0));
        del = del.max(ctx.hlc.now());
    }
    ctx.hlc.observe(del);
    for change in delta {
        match change {
            Delta::Map {
                path,
                val,
                observed,
            } => write_map_node(ctx, key, path, val, observed),
            Delta::Remove { path, observed } => remove_map_node(ctx, key, path, observed),
            Delta::Array { path, elem, live } => {
                if *live {
                    write_arr_node(ctx, key, path, elem)
                } else {
                    tomb_arr_node(ctx, key, path, elem)
                }
            }
        }
    }
    if initialize
        || !previous.is_some_and(|(env, t, _)| {
            !env.is_tombstone()
                && !env.is_expired(now)
                && env.ttl_deadline_ms == 0
                && t == head::CTYPE_JSON
        })
    {
        let rec = Envelope::head(ctx.hlc.now(), ctx.node_id)
            .encode_with(&head::encode(head::CTYPE_JSON, del));
        store::write_merged(ctx, &ikey::head_key(key), &rec);
    }
}
fn verify_inputs(
    ctx: &ShardCtx,
    source: &[u8],
    source_revision: [u8; 32],
    dest: &[u8],
    dest_revision: [u8; 32],
    config: &DiffConfig,
) -> Result<(), Reply> {
    if snapshot::revision(ctx, source, config)? != source_revision {
        return Err(Reply::err("DIFFSTALE import source changed"));
    }
    if snapshot::revision(ctx, dest, config)? != dest_revision {
        return Err(Reply::err("DIFFSTALE import destination changed"));
    }
    Ok(())
}

pub async fn import(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("diff.import");
    }
    let dest = match DiffKey::parse(&args[1]) {
        Ok(k @ DiffKey::Branch { .. }) => k,
        Ok(_) => return Reply::err("DIFFKEY import destination must be a branch"),
        Err(e) => return e,
    };
    let mut source = args[1].clone();
    let mut theta = 0.7f32;
    let mut seen_from = false;
    let mut seen_theta = false;
    let mut i = 3;
    while i < args.len() {
        if i + 1 >= args.len() {
            return Reply::syntax();
        }
        if args[i].eq_ignore_ascii_case(b"FROM") && !seen_from {
            source = args[i + 1].clone();
            seen_from = true;
        } else if args[i].eq_ignore_ascii_case(b"THETA") && !seen_theta {
            theta = match std::str::from_utf8(&args[i + 1])
                .ok()
                .and_then(|s| s.parse::<f32>().ok())
            {
                Some(v) if v.is_finite() && (0.0..=1.0).contains(&v) => v,
                _ => return Reply::not_float(),
            };
            seen_theta = true;
        } else {
            return Reply::syntax();
        }
        i += 2;
    }
    let src = match DiffKey::parse(&source) {
        Ok(k @ (DiffKey::Branch { .. } | DiffKey::Snapshot { .. })) => k,
        Ok(_) => return Reply::err("DIFFKEY import FROM must be a branch or snapshot"),
        Err(e) => return e,
    };
    if src.tag() != dest.tag() {
        return Reply::err("CROSSSLOT DIFF keys must share one tag");
    }
    let tag = dest.tag().to_owned();
    let config = engine.diff.config.clone();
    if args[2].len() > config.max_bytes {
        return Reply::err("DIFFTOOBIG upload bytes");
    }
    let bytes = config
        .max_bytes
        .saturating_mul(2)
        .saturating_add(args[2].len());
    let admission = match engine.diff.pool.admit(bytes) {
        Ok(g) => g,
        Err(e) => return e,
    };
    let _cancel = admission.cancel_on_drop();
    engine.ensure_local(&source).await;
    engine.ensure_local(&args[1]).await;
    let source_key = source.clone();
    let dest_key = args[1].clone();
    let cfg = config.clone();
    let capture_admission = admission.clone();
    let captured = engine
        .store
        .run_key(&args[1], move |ctx| -> Result<_, Reply> {
            let _guard = capture_admission;
            _guard.cancel.check().map_err(super::error)?;
            let source_revision = snapshot::revision(ctx, &source_key, &cfg)?;
            let dest_revision = snapshot::revision(ctx, &dest_key, &cfg)?;
            let source = if store::key_type(ctx, &source_key).is_some() {
                Some(snapshot::capture(ctx, &source_key, &cfg)?)
            } else if seen_from {
                return Err(Reply::err("DIFFNOSNAPSHOT import source missing"));
            } else {
                None
            };
            let destination = if source_key == dest_key {
                source.clone()
            } else if store::key_type(ctx, &dest_key) == Some(head::CTYPE_JSON) {
                Some(snapshot::capture(ctx, &dest_key, &cfg)?)
            } else {
                None
            };
            Ok((source, destination, source_revision, dest_revision))
        })
        .await;
    let (source_input, dest_input, source_revision, dest_revision) = match captured {
        Ok(v) => v,
        Err(e) => return e,
    };
    let input = args[2].clone();
    let cfg = config.clone();
    let hlc = engine.store.hlc.clone();
    let origin = engine.store.node_id;
    let computed = engine
        .diff
        .pool
        .run(admission.clone(), move |cancel| {
            cancel.check()?;
            let b = Tree::from_json_with_budget(
                &serde_json::from_slice(&input)
                    .map_err(|_| DiffError::InvalidGraph("invalid upload JSON".into()))?,
                &cfg.budget,
            )?;
            let a = match &source_input {
                Some(s) => snapshot::decode(s, &cfg.budget)?,
                None => Tree::from_json(&serde_json::json!({"t":"doc","c":[]}))?,
            };
            let options = marekvs_diff::Options {
                theta,
                budget: cfg.budget.clone(),
                cancel: cancel.clone(),
                ..Default::default()
            };
            let (g, m) = marekvs_diff::diff_with_matching(&a, &b, &options)?;
            let source_nodes = source_input
                .as_ref()
                .map(|s| s.nodes.as_slice())
                .unwrap_or(&[]);
            let destination = dest_input
                .as_ref()
                .map(|s| s.nodes.as_slice())
                .unwrap_or(&[]);
            let desired = desired_records(
                &a,
                &b,
                (&m, &g),
                source_nodes,
                &mut || Eid {
                    hlc: hlc.now(),
                    origin,
                },
                &cfg.budget,
                cancel,
            )?;
            let delta = deltas(source_nodes, destination, desired, cancel)?;
            if delta.len() > cfg.max_records {
                return Err(DiffError::TooBig {
                    bound: "import_records",
                    n: delta.len(),
                    limit: cfg.max_records,
                });
            }
            Ok((a, b, g, delta, source_input, dest_input))
        })
        .await;
    let (a, b, g, delta, source_input, dest_input) = match computed {
        Ok(v) => v,
        Err(e) => return e,
    };
    let dest_key = args[1].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let _guard = admission;
            if let Err(e) = _guard.cancel.check() {
                return super::error(e);
            }
            for input in [&source_input, &dest_input].into_iter().flatten() {
                if let Err(e) = snapshot::verify_revision(ctx, input, &config) {
                    return e;
                }
            }
            if let Err(e) = verify_inputs(
                ctx,
                &source,
                source_revision,
                &dest_key,
                dest_revision,
                &config,
            ) {
                return e;
            }
            if let Err(e) = _guard.cancel.check() {
                return super::error(e);
            }
            let stats = serde_json::to_vec(&g.stats).expect("stats serialize");
            let stored = StoredGraph::Two(g);
            if let Err(e) = snapshot::write_snapshot(ctx, &tag, &a) {
                return e;
            }
            if let Err(e) = snapshot::write_snapshot(ctx, &tag, &b) {
                return e;
            }
            let graph = match compare::publish_graph(ctx, &tag, &stored) {
                Ok(k) => k,
                Err(e) => return e,
            };
            write_delta(ctx, &dest_key, &delta, dest_input.is_none());
            Reply::Map(vec![
                (
                    Reply::bulk_str("sid"),
                    Reply::bulk_str(format!("{:032x}", b.sid.0)),
                ),
                (Reply::bulk_str("gid"), Reply::Bulk(graph)),
                (Reply::bulk_str("stats"), Reply::Bulk(stats)),
                (Reply::bulk_str("writes"), Reply::Int(delta.len() as i64)),
            ])
        })
        .await
}

#[cfg(test)]
mod publication_tests {
    use super::*;
    #[tokio::test]
    async fn publication_rechecks_both_source_and_destination_revisions() {
        let dir = tempfile::tempdir().unwrap();
        let store = store::Store::open(&store::StoreConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            node_id: 11,
            shard_threads: 1,
            ..Default::default()
        })
        .unwrap();
        let e = Engine::new(store);
        let src = b"doc:{race}:b:source".to_vec();
        let dst = b"doc:{race}:b:dest".to_vec();
        let json = br#"{"t":"doc","c":[]}"#.to_vec();
        for key in [&src, &dst] {
            assert_eq!(
                crate::cmd::json::set(
                    &e,
                    &[
                        b"JSON.SET".to_vec(),
                        key.clone(),
                        b"$".to_vec(),
                        json.clone()
                    ]
                )
                .await,
                Reply::ok()
            );
        }
        for mutate_source in [true, false] {
            let (s, d) = (src.clone(), dst.clone());
            let cfg = e.diff.config.clone();
            let (sr, dr) = e
                .store
                .run_key(&src, move |ctx| {
                    (
                        snapshot::revision(ctx, &s, &cfg).unwrap(),
                        snapshot::revision(ctx, &d, &cfg).unwrap(),
                    )
                })
                .await;
            let key = if mutate_source { &src } else { &dst };
            let text = if mutate_source {
                "source changed"
            } else {
                "destination changed"
            };
            let changed =
                serde_json::to_vec(&serde_json::json!({"t":"doc","c":[{"t":"sen","x":text}]}))
                    .unwrap();
            assert_eq!(
                crate::cmd::json::set(
                    &e,
                    &[b"JSON.SET".to_vec(), key.clone(), b"$".to_vec(), changed]
                )
                .await,
                Reply::ok()
            );
            let (s, d) = (src.clone(), dst.clone());
            let cfg = e.diff.config.clone();
            let result = e
                .store
                .run_key(&src, move |ctx| verify_inputs(ctx, &s, sr, &d, dr, &cfg))
                .await;
            assert!(matches!(result,Err(Reply::Err(ref e))if e.starts_with("DIFFSTALE")));
        }
    }
    #[test]
    fn decomposition_limits_and_cancellation_precede_accumulated_records() {
        let v = serde_json::json!({"t":"doc","c":[{"t":"sen","x":"hello"}]});
        let mut n = 10;
        let mut fresh = |_: &[u8], _: usize| {
            n += 1;
            Eid { hlc: n, origin: 1 }
        };
        assert!(decompose_bounded(
            &v,
            &mut fresh,
            &Budget {
                max_bytes: 100,
                ..Default::default()
            },
            &CancelToken::none()
        )
        .is_err());
        let cancel = CancelToken::none();
        cancel.cancel();
        assert!(matches!(
            decompose_bounded(&v, &mut fresh, &Budget::default(), &cancel),
            Err(DiffError::Cancelled)
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn source(v: serde_json::Value) -> (Tree, Vec<(Vec<u8>, NodeIn)>) {
        let mut next = 10;
        let rs = json::decompose(&[], &v, &mut || {
            next += 1;
            Eid {
                hlc: next,
                origin: 7,
            }
        });
        let ns = rs
            .into_iter()
            .map(|r| match r {
                JsonRecord::Map { path, val } => (path, NodeIn::Map { val, dots: vec![] }),
                JsonRecord::Arr { path, elem } => (path, NodeIn::ArrElem { elem, live: true }),
            })
            .collect::<Vec<_>>();
        let t = marekvs_diff::records::from_snapshot(&ns).unwrap();
        (t, ns)
    }
    #[test]
    fn stable_and_moved_subtrees_have_complete_records() {
        let (a, ns) = source(
            json!({"t":"doc","c":[{"t":"sec","a":{"title":"A"},"c":[{"t":"sen","x":"one"}]},{"t":"sec","a":{"title":"B"},"c":[{"t":"sen","x":"two"}]}]}),
        );
        let b=Tree::from_json(&json!({"t":"doc","c":[{"t":"sec","a":{"title":"B"},"c":[{"t":"sen","x":"two"}]},{"t":"sec","a":{"title":"A"},"c":[{"t":"sen","x":"one"}]}]})).unwrap();
        let (g, m) = marekvs_diff::diff_with_matching(&a, &b, &Default::default()).unwrap();
        let mut next = 100;
        let rs = desired_records(
            &a,
            &b,
            (&m, &g),
            &ns,
            &mut || {
                next += 1;
                Eid {
                    hlc: next,
                    origin: 7,
                }
            },
            &Default::default(),
            &CancelToken::none(),
        )
        .unwrap();
        let mut desired = rs
            .into_iter()
            .map(|r| match r {
                JsonRecord::Map { path, val } => (path, NodeIn::Map { val, dots: vec![] }),
                JsonRecord::Arr { path, elem } => (path, NodeIn::ArrElem { elem, live: true }),
            })
            .collect::<Vec<_>>();
        let live: BTreeSet<_> = desired.iter().map(|(p, _)| p.clone()).collect();
        for (p, n) in ns {
            if !live.contains(&p) {
                if let NodeIn::ArrElem { elem, .. } = n {
                    desired.push((p, NodeIn::ArrElem { elem, live: false }));
                }
            }
        }
        assert_eq!(json::build_doc(&desired).unwrap().value, b.to_json());
    }
}

#[cfg(test)]
mod ordering_tests {
    use super::*;
    #[test]
    fn all_small_insert_delete_and_reorder_sequences_materialize_exactly() {
        let base = serde_json::json!({"t":"doc","c":[{"t":"sen","x":"Alpha."},{"t":"sen","x":"Beta."},{"t":"sen","x":"Gamma."}]});
        let mut n = 10;
        let records = json::decompose(&[], &base, &mut || {
            n += 1;
            Eid { hlc: n, origin: 1 }
        });
        let source: Vec<_> = records
            .into_iter()
            .map(|r| match r {
                JsonRecord::Map { path, val } => (path, NodeIn::Map { val, dots: vec![] }),
                JsonRecord::Arr { path, elem } => (path, NodeIn::ArrElem { elem, live: true }),
            })
            .collect();
        let a = marekvs_diff::records::from_snapshot(&source).unwrap();
        let words = ["Alpha.", "Beta.", "Gamma.", "Delta."];
        fn visit(prefix: &mut Vec<usize>, f: &mut dyn FnMut(&[usize])) {
            f(prefix);
            for i in 0..4 {
                if !prefix.contains(&i) {
                    prefix.push(i);
                    visit(prefix, f);
                    prefix.pop();
                }
            }
        }
        visit(&mut vec![], &mut |order| {
            let v = serde_json::json!({"t":"doc","c":order.iter().map(|&i|serde_json::json!({"t":"sen","x":words[i]})).collect::<Vec<_>>()});
            let b = Tree::from_json(&v).unwrap();
            let (g, m) = marekvs_diff::diff_with_matching(&a, &b, &Default::default()).unwrap();
            let mut fresh = 100;
            let records = desired_records(
                &a,
                &b,
                (&m, &g),
                &source,
                &mut || {
                    fresh += 1;
                    Eid {
                        hlc: fresh,
                        origin: 1,
                    }
                },
                &Default::default(),
                &CancelToken::none(),
            )
            .unwrap();
            let mut nodes: Vec<_> = records
                .into_iter()
                .map(|r| match r {
                    JsonRecord::Map { path, val } => (path, NodeIn::Map { val, dots: vec![] }),
                    JsonRecord::Arr { path, elem } => (path, NodeIn::ArrElem { elem, live: true }),
                })
                .collect();
            let live: BTreeSet<_> = nodes.iter().map(|(p, _)| p.clone()).collect();
            for (p, node) in &source {
                if !live.contains(p) {
                    if let NodeIn::ArrElem { elem, .. } = node {
                        nodes.push((
                            p.clone(),
                            NodeIn::ArrElem {
                                elem: elem.clone(),
                                live: false,
                            },
                        ));
                    }
                }
            }
            assert_eq!(
                json::build_doc(&nodes).unwrap().value,
                b.to_json(),
                "order={order:?}"
            );
        });
    }
}
