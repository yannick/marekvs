//! Bounded shard capture and deterministic immutable snapshot publication.
use super::{
    keys::{self, DiffKey},
    pool::DiffConfig,
};
use crate::{
    reply::Reply,
    store::{self, ShardCtx},
    Engine,
};
use marekvs_core::{
    envelope::{head, Envelope, RecordType},
    ikey,
    json::{self, ArrElem, Eid, JVal, JsonRecord, NodeIn, Seg},
    merge::{element_add_with_dot, Dot, ElementState},
};
use marekvs_diff::{Budget, DiffError, Sid, Tree};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use xxhash_rust::xxh3::xxh3_128;

#[derive(Clone, Debug)]
pub(crate) struct Captured {
    pub key: Vec<u8>,
    pub nodes: Vec<(Vec<u8>, NodeIn)>,
    pub revision: [u8; 32],
    pub expected_sid: Option<Sid>,
    pub physical: usize,
    pub live: usize,
    pub bytes: usize,
}
fn revision_add(rev: &mut [u8; 32], key: &[u8], value: &[u8]) {
    let mut h = xxhash_rust::xxh3::Xxh3::new();
    h.update(rev);
    h.update(&(key.len() as u64).to_be_bytes());
    h.update(key);
    h.update(&(value.len() as u64).to_be_bytes());
    h.update(value);
    let digest = h.digest128().to_be_bytes();
    rev.copy_within(0..16, 16);
    rev[..16].copy_from_slice(&digest);
}
/// Revision of any destination, including absent heads and partial artifacts.
pub(crate) fn revision(ctx: &ShardCtx, key: &[u8], config: &DiffConfig) -> Result<[u8; 32], Reply> {
    let mut rev = [0; 32];
    let mut bytes = 0usize;
    let mut physical = 0usize;
    let mut over = false;
    for k in [
        ikey::head_key(key),
        ikey::string_key(key),
        ikey::list_key(key),
    ] {
        if let Some(v) = store::get_raw(ctx, &k) {
            bytes = bytes.saturating_add(k.len()).saturating_add(v.len());
            revision_add(&mut rev, &k, &v);
        }
    }
    // FORK REPLACE can replace any branch-key type; member edits need not
    // update its head. Include every collection family in the revision.
    for tag in [
        ikey::Tag::HashField,
        ikey::Tag::SetMember,
        ikey::Tag::ZsetMember,
        ikey::Tag::ZsetScore,
        ikey::Tag::ListElem,
        ikey::Tag::HllRegister,
        ikey::Tag::StreamEntry,
        ikey::Tag::Budget,
        ikey::Tag::Json,
        ikey::Tag::ProtoField,
    ] {
        if over {
            break;
        }
        store::scan_prefix(ctx, &ikey::collection_prefix(tag, key), |k, v| {
            physical += 1;
            bytes = bytes.saturating_add(k.len()).saturating_add(v.len());
            if physical > config.max_physical || bytes > config.max_bytes {
                over = true;
                return false;
            }
            revision_add(&mut rev, k, v);
            true
        })
        .map_err(|e| Reply::err(format!("ERR {e}")))?;
    }
    if over || bytes > config.max_bytes {
        Err(Reply::err("DIFFTOOBIG destination records or bytes"))
    } else {
        Ok(rev)
    }
}

fn missing() -> Reply {
    Reply::err("DIFFNOSNAPSHOT missing or incomplete structured document")
}
pub(crate) fn capture(ctx: &ShardCtx, key: &[u8], config: &DiffConfig) -> Result<Captured, Reply> {
    let parsed = DiffKey::parse(key)?;
    let expected_sid = match parsed {
        DiffKey::Snapshot { name, .. } => {
            Some(Sid(u128::from_str_radix(&name, 16).map_err(|_| missing())?))
        }
        DiffKey::Branch { .. } => None,
        _ => return Err(Reply::err("DIFFKEY expected branch or snapshot")),
    };
    let (head_env, ctype, del) = store::get_head(ctx, key).ok_or_else(missing)?;
    if head_env.is_tombstone() || head_env.is_expired(store::now_ms()) {
        return Err(missing());
    }
    if ctype != head::CTYPE_JSON || store::check_type(ctx, key, head::CTYPE_JSON).is_err() {
        return Err(if expected_sid.is_some() {
            missing()
        } else {
            Reply::wrongtype()
        });
    }
    let mut out = Captured {
        key: key.to_vec(),
        nodes: vec![],
        revision: [0; 32],
        expected_sid,
        physical: 0,
        live: 0,
        bytes: 0,
    };
    let head_key = ikey::head_key(key);
    let head_raw = store::get_raw(ctx, &head_key).unwrap();
    out.bytes = head_key.len().saturating_add(head_raw.len());
    if out.bytes > config.max_bytes {
        return Err(Reply::err("DIFFTOOBIG head bytes"));
    }
    revision_add(&mut out.revision, &head_key, &head_raw);
    let now = store::now_ms();
    let mut failure = None;
    store::scan_prefix(
        ctx,
        &ikey::collection_prefix(ikey::Tag::Json, key),
        |k, v| {
            out.physical = out.physical.saturating_add(1);
            out.bytes = out.bytes.saturating_add(k.len()).saturating_add(v.len());
            if out.physical > config.max_physical || out.bytes > config.max_bytes {
                failure = Some(Reply::err("DIFFTOOBIG physical records or bytes"));
                return false;
            }
            revision_add(&mut out.revision, k, v);
            let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) else {
                return true;
            };
            if env.hlc <= del || env.is_expired(now) {
                return true;
            }
            let node = if matches!(json::split_last(p.suffix), Some((_, Seg::Elem(_)))) {
                ArrElem::decode(pay).map(|elem| NodeIn::ArrElem {
                    elem,
                    live: !env.is_tombstone(),
                })
            } else if !env.is_tombstone() {
                ElementState::decode(pay).and_then(|st| {
                    Some(NodeIn::Map {
                        val: JVal::decode(st.value()?)?,
                        dots: st.dots(),
                    })
                })
            } else {
                None
            };
            if let Some(node) = node {
                if !matches!(node, NodeIn::ArrElem { live: false, .. }) {
                    out.live += 1;
                }
                if out.live > config.max_records {
                    failure = Some(Reply::err("DIFFTOOBIG live records"));
                    return false;
                }
                out.nodes.push((p.suffix.to_vec(), node));
            }
            true
        },
    )
    .map_err(|e| Reply::err(format!("ERR {e}")))?;
    if let Some(e) = failure {
        Err(e)
    } else {
        Ok(out)
    }
}
pub(crate) fn decode(input: &Captured, budget: &Budget) -> Result<Tree, DiffError> {
    let (mut tree, _) = marekvs_diff::records::snapshot_with_binding(&input.nodes, budget)
        .map_err(|e| {
            if input.expected_sid.is_some() {
                DiffError::InvalidGraph("DIFFNOSNAPSHOT incomplete snapshot".into())
            } else {
                e.into()
            }
        })?;
    if let Some(sid) = input.expected_sid {
        if tree.sid != sid {
            return Err(DiffError::InvalidGraph(
                "DIFFNOSNAPSHOT incomplete snapshot".into(),
            ));
        }
        for node in &mut tree.nodes {
            node.eid = None;
        }
    }
    Ok(tree)
}
pub(crate) fn verify_revision(
    ctx: &ShardCtx,
    input: &Captured,
    config: &DiffConfig,
) -> Result<(), Reply> {
    let current =
        capture(ctx, &input.key, config).map_err(|_| Reply::err("DIFFSTALE source changed"))?;
    if current.revision != input.revision {
        Err(Reply::err("DIFFSTALE source changed"))
    } else {
        Ok(())
    }
}
pub(crate) fn snapshot_key(tag: &str, sid: Sid) -> Vec<u8> {
    keys::snapshot_key(tag, sid)
}
fn identity(sid: Sid, path: &[u8], ordinal: usize, domain: &[u8]) -> u64 {
    let mut b = domain.to_vec();
    b.extend_from_slice(&sid.0.to_be_bytes());
    b.extend_from_slice(&(path.len() as u64).to_be_bytes());
    b.extend_from_slice(path);
    b.extend_from_slice(&(ordinal as u64).to_be_bytes());
    // Reserved low clocks leave modern ordinary JSON inserts ahead of these anchors.
    (xxh3_128(&b) as u64 & ((1u64 << 48) - 1)).max(1)
}
pub(crate) fn canonical_records(tree: &Tree) -> Result<Vec<JsonRecord>, Reply> {
    let mut ids = BTreeSet::new();
    let mut collision = false;
    let records = json::decompose_with_paths(&[], &tree.to_json(), &mut |path, ordinal| {
        let eid = Eid {
            hlc: identity(tree.sid, path, ordinal, b"diff/eid/v1"),
            origin: 0,
        };
        if !ids.insert(eid) {
            collision = true;
        }
        eid
    });
    if collision {
        Err(Reply::err("DIFFCOLLISION deterministic element identity"))
    } else {
        Ok(records)
    }
}
pub(crate) fn write_snapshot(ctx: &ShardCtx, tag: &str, tree: &Tree) -> Result<Vec<u8>, Reply> {
    let key = snapshot_key(tag, tree.sid);
    if store::check_type(ctx, &key, head::CTYPE_JSON).is_err() {
        return Err(Reply::err("DIFFCOLLISION snapshot destination type"));
    }
    let records = canonical_records(tree)?;
    let mut dots = BTreeSet::new();
    let mut expected = BTreeMap::new();
    for r in records {
        let (path, rtype, payload) = match r {
            JsonRecord::Map { path, val } => {
                let dot = Dot {
                    hlc: identity(tree.sid, &path, 0, b"diff/dot/v1"),
                    origin: 0,
                };
                if !dots.insert((dot.hlc, dot.origin)) {
                    return Err(Reply::err("DIFFCOLLISION deterministic dot"));
                }
                let payload = ElementState {
                    live: vec![(dot, val.encode())],
                    covered: vec![],
                }
                .encode();
                (path, RecordType::HashField, payload)
            }
            JsonRecord::Arr { path, elem } => (path, RecordType::List, elem.encode()),
        };
        expected.insert(path, (rtype, payload));
    }
    let mut conflict = false;
    // A missing subset is repairable. A foreign record is never overwritten.
    store::scan_prefix(
        ctx,
        &ikey::collection_prefix(ikey::Tag::Json, &key),
        |k, v| {
            let valid = ikey::parse(k)
                .and_then(|p| expected.get(p.suffix))
                .zip(Envelope::decode(v))
                .is_some_and(|((t, want), (env, pay))| {
                    env.rtype() == *t && !env.is_tombstone() && pay == want
                });
            if !valid {
                conflict = true;
                return false;
            }
            true
        },
    )
    .map_err(|e| Reply::err(format!("ERR {e}")))?;
    if conflict {
        return Err(Reply::err(
            "DIFFCOLLISION snapshot contains noncanonical records",
        ));
    }
    let now = store::now_ms();
    let old = store::get_head(ctx, &key);
    let del = old.map_or(0, |(env, _, d)| {
        d.max(if env.is_tombstone() { env.hlc } else { 0 })
            .max(if env.is_expired(now) {
                env.expiry_hlc()
            } else {
                0
            })
    });
    ctx.hlc.observe(del);
    let mut changed = false;
    for (path, (rtype, payload)) in expected {
        let rk = ikey::json_node_key(&key, &path);
        let valid = store::get_raw(ctx, &rk)
            .and_then(|v| {
                Envelope::decode(&v).map(|(env, pay)| {
                    env.hlc > del
                        && env.ttl_deadline_ms == 0
                        && !env.is_tombstone()
                        && pay == payload
                })
            })
            .unwrap_or(false);
        if valid {
            continue;
        }
        let hlc = ctx.hlc.now().max(del.saturating_add(1));
        let rec = if rtype == RecordType::HashField {
            let st = ElementState::decode(&payload).unwrap();
            element_add_with_dot(rtype, hlc, ctx.node_id, st.live[0].0, &st.live[0].1)
        } else {
            Envelope::new(rtype, hlc, ctx.node_id).encode_with(&payload)
        };
        store::write_merged(ctx, &rk, &rec);
        let complete = store::get_raw(ctx, &rk)
            .and_then(|v| {
                Envelope::decode(&v).map(|(env, pay)| {
                    env.hlc > del
                        && env.ttl_deadline_ms == 0
                        && !env.is_tombstone()
                        && pay == payload
                })
            })
            .unwrap_or(false);
        if !complete {
            return Err(Reply::err("DIFFSTORAGE snapshot record publication failed"));
        }
        changed = true;
    }
    if changed
        || !old.is_some_and(|(e, t, d)| {
            !e.is_tombstone()
                && !e.is_expired(now)
                && e.ttl_deadline_ms == 0
                && t == head::CTYPE_JSON
                && d == del
        })
    {
        let rec = Envelope::head(ctx.hlc.now().max(del.saturating_add(1)), ctx.node_id)
            .encode_with(&head::encode(head::CTYPE_JSON, del));
        store::write_merged(ctx, &ikey::head_key(&key), &rec);
    }
    Ok(key)
}
pub(crate) fn observe_records(metrics: &crate::metrics::DiffMetrics, input: &Captured) {
    metrics
        .records
        .with_label_values(&["physical"])
        .inc_by(input.physical as u64);
    metrics
        .records
        .with_label_values(&["live"])
        .inc_by(input.live as u64);
}

pub async fn snapshot(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("diff.snapshot");
    }
    let parsed = match DiffKey::parse(&args[1]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let tag = parsed.tag().to_owned();
    let config = engine.diff.config.clone();
    let admission = match engine.diff.pool.admit(config.max_bytes) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let _cancel = admission.cancel_on_drop();
    engine.ensure_local(&args[1]).await;
    let key = args[1].clone();
    let scan_config = config.clone();
    let scan_admission = admission.clone();
    let scan_metrics = engine.diff.metrics.clone();
    let captured = match engine
        .store
        .run_key(&args[1], move |ctx| {
            let _guard = scan_admission;
            let _timer = scan_metrics
                .duration
                .with_label_values(&["scan"])
                .start_timer();
            _guard.cancel.check().map_err(super::error)?;
            let input = capture(ctx, &key, &scan_config)?;
            observe_records(&scan_metrics, &input);
            Ok(input)
        })
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    let budget = config.budget.clone();
    let input = captured.clone();
    let tree = match engine
        .diff
        .pool
        .run(admission.clone(), move |cancel| {
            cancel.check()?;
            decode(&input, &budget)
        })
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let _guard = admission;
            if let Err(e) = _guard.cancel.check() {
                return super::error(e);
            }
            if let Err(e) = verify_revision(ctx, &captured, &config) {
                return e;
            }
            if let Err(e) = _guard.cancel.check() {
                return super::error(e);
            }
            write_snapshot(ctx, &tag, &tree)
                .map(Reply::Bulk)
                .unwrap_or_else(|e| e)
        })
        .await
}

#[cfg(test)]
mod measurements {
    use super::*;
    #[tokio::test]
    async fn capture_bounds_head_and_record_allocations() {
        let dir = tempfile::tempdir().unwrap();
        let store = store::Store::open(&store::StoreConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            node_id: 1,
            shard_threads: 2,
            ..store::StoreConfig::default()
        })
        .unwrap();
        let engine = Engine::new(store);
        let key = b"doc:{bounds}:b:a";
        assert_eq!(
            crate::cmd::json::set(
                &engine,
                &[
                    b"JSON.SET".to_vec(),
                    key.to_vec(),
                    b"$".to_vec(),
                    br#"{"t":"doc","c":[{"t":"sen","x":"hello"}]}"#.to_vec()
                ]
            )
            .await,
            Reply::ok()
        );
        for bound in 0..3 {
            let mut config = engine.diff.config.clone();
            match bound {
                0 => config.max_physical = 1,
                1 => config.max_records = 1,
                _ => config.max_bytes = 1,
            }
            let result = engine
                .store
                .run_key(key, move |ctx| capture(ctx, key, &config))
                .await;
            assert!(matches!(result,Err(Reply::Err(e))if e.starts_with("DIFFTOOBIG")));
        }
    }

    #[tokio::test]
    #[ignore = "manual scan/decode measurement; reports profile and fixture sizes"]
    async fn scan_decode_measurement() {
        let dir = tempfile::tempdir().unwrap();
        let store = store::Store::open(&store::StoreConfig {
            data_dir: dir.path().to_string_lossy().into_owned(),
            node_id: 1,
            shard_threads: 2,
            ..store::StoreConfig::default()
        })
        .unwrap();
        let engine = Engine::new(store);
        for count in [10, 100, 1000] {
            let value = serde_json::json!({"t":"doc","c":(0..count).map(|i|serde_json::json!({"t":"sen","x":format!("Clause {i} describes a representative document sentence."),"a":{"role":"body"}})).collect::<Vec<_>>()});
            let key = format!("doc:{{measure}}:b:{count}").into_bytes();
            assert_eq!(
                crate::cmd::json::set(
                    &engine,
                    &[
                        b"JSON.SET".to_vec(),
                        key.clone(),
                        b"$".to_vec(),
                        serde_json::to_vec(&value).unwrap()
                    ]
                )
                .await,
                Reply::ok()
            );
            let config = engine.diff.config.clone();
            let input_key = key.clone();
            let (captured, scan) = engine
                .store
                .run_key(&key, move |ctx| {
                    let start = std::time::Instant::now();
                    let captured = capture(ctx, &input_key, &config).unwrap();
                    (captured, start.elapsed())
                })
                .await;
            let decoded_bytes: usize = captured
                .nodes
                .iter()
                .map(|(path, node)| {
                    path.len()
                        + match node {
                            NodeIn::Map { val, .. } => val.encode().len(),
                            NodeIn::ArrElem { elem, .. } => elem.encode().len(),
                        }
                })
                .sum();
            let bytes = captured.bytes;
            let physical = captured.physical;
            let live = captured.live;
            let admission = engine
                .diff
                .pool
                .admit(engine.diff.config.max_bytes)
                .unwrap();
            let budget = engine.diff.config.budget.clone();
            let decode = engine
                .diff
                .pool
                .run(admission, move |cancel| {
                    cancel.check()?;
                    let start = std::time::Instant::now();
                    let tree = decode(&captured, &budget)?;
                    std::hint::black_box(tree);
                    Ok(start.elapsed())
                })
                .await
                .unwrap();
            println!("children={count} physical={physical} live={live} scanned_bytes={bytes} decoded_record_bytes={decoded_bytes} scan_us={} decode_us={} scan_records_per_s={:.0} decode_scanned_MiB_per_s={:.2}",scan.as_micros(),decode.as_micros(),physical as f64/scan.as_secs_f64(),bytes as f64/1048576.0/decode.as_secs_f64());
        }
    }
}
