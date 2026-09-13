//! Idempotent application and three-way graph publication.
use super::{
    compare::{self, StoredGraph},
    decide,
    keys::DiffKey,
    snapshot,
};
use crate::{
    reply::Reply,
    store::{self, ShardCtx},
    Engine,
};
use marekvs_core::ikey;
use marekvs_diff::{ChangeId, Tree};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResultRecord {
    pub gid: String,
    pub drev: String,
    pub sid: String,
    pub mapping: BTreeMap<String, Vec<u32>>,
    pub at: u64,
}
fn result_reply(rid: &str, result: &ResultRecord) -> Reply {
    Reply::Map(vec![
        (Reply::bulk_str("rid"), Reply::bulk_str(rid)),
        (Reply::bulk_str("sid"), Reply::bulk_str(&result.sid)),
        (Reply::bulk_str("unresolved"), Reply::Array(vec![])),
    ])
}
fn unresolved_reply(groups: Vec<Vec<ChangeId>>) -> Reply {
    Reply::Map(vec![(
        Reply::bulk_str("unresolved"),
        Reply::Array(
            groups
                .into_iter()
                .map(|g| Reply::Array(g.into_iter().map(|id| Reply::bulk_str(id.0)).collect()))
                .collect(),
        ),
    )])
}
fn read_result(
    ctx: &ShardCtx,
    key: &[u8],
    graph: &str,
    max_bytes: usize,
) -> Result<Option<ResultRecord>, Reply> {
    let Some((_, bytes)) = store::read_lww(ctx, &ikey::string_key(key), 0) else {
        if store::key_type(ctx, key).is_some() {
            return Err(Reply::wrongtype());
        }
        return Ok(None);
    };
    if bytes.len() > max_bytes {
        return Err(Reply::err("DIFFTOOBIG result bytes"));
    }
    let result: ResultRecord = serde_json::from_slice(&bytes)
        .map_err(|_| Reply::err("DIFFINVALID malformed request result"))?;
    if result.gid != graph {
        return Err(Reply::err("DIFFREQUEST token belongs to another graph"));
    }
    Ok(Some(result))
}
pub async fn apply(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if !(args.len() == 4 || args.len() == 6) || !args[2].eq_ignore_ascii_case(b"REQUEST") {
        return Reply::wrong_args("diff.apply");
    }
    let tag = match compare::graph_tag(&args[1]) {
        Ok(t) => t,
        Err(e) => return e,
    };
    let rid = match std::str::from_utf8(&args[3]) {
        Ok(s) if !s.is_empty() && s.len() <= 256 => s.to_owned(),
        _ => return Reply::err("DIFFREQUEST invalid request token"),
    };
    let result_key = format!("diff:{{{tag}}}:r:{rid}").into_bytes();
    if let Err(e) = DiffKey::parse(&result_key) {
        return e;
    }
    let expected = if args.len() == 6 {
        if !args[4].eq_ignore_ascii_case(b"DECISIONS") {
            return Reply::syntax();
        }
        Some(String::from_utf8_lossy(&args[5]).into_owned())
    } else {
        None
    };
    let admission = match engine
        .diff
        .pool
        .admit(engine.diff.config.max_bytes.saturating_mul(3))
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    let _cancel = admission.cancel_on_drop();
    // Durable request identity is consulted before current snapshots/decisions.
    engine.ensure_local(&result_key).await;
    let graph_name = String::from_utf8_lossy(&args[1]).into_owned();
    let key = result_key.clone();
    let gname = graph_name.clone();
    let cfg = engine.diff.config.clone();
    let permit = admission.clone();
    let existing = engine
        .store
        .run_key(&key.clone(), move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            read_result(ctx, &key, &gname, cfg.max_bytes)
        })
        .await;
    match existing {
        Ok(Some(result)) => return result_reply(&rid, &result),
        Err(e) => return e,
        Ok(None) => {}
    }
    engine.ensure_local(&args[1]).await;
    let gkey = args[1].clone();
    let cfg = engine.diff.config.clone();
    let permit = admission.clone();
    let stored = engine
        .store
        .run_key(&gkey.clone(), move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            compare::read_graph(ctx, &gkey, cfg.max_bytes)
        })
        .await;
    let stored = match stored {
        Ok(g) => g,
        Err(e) => return e,
    };
    let mut input_keys = Vec::new();
    for sid in stored.inputs() {
        let key = super::keys::snapshot_key(&tag, sid);
        if !input_keys.contains(&key) {
            input_keys.push(key);
        }
    }
    for key in &input_keys {
        engine.ensure_local(key).await;
    }
    engine
        .ensure_local(&decide::decision_key(&tag, &stored))
        .await;
    let cfg = engine.diff.config.clone();
    let permit = admission.clone();
    let read_tag = tag.clone();
    let source_graph = stored.clone();
    let captured = engine
        .store
        .run_key(&args[1], move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            let mut inputs = Vec::new();
            for key in input_keys {
                inputs.push(snapshot::capture(ctx, &key, &cfg)?);
            }
            let view = decide::read_view(ctx, &read_tag, &source_graph, cfg.max_bytes)?;
            Ok::<_, Reply>((inputs, view))
        })
        .await;
    let (inputs, view) = match captured {
        Ok(v) => v,
        Err(e) => return e,
    };
    if expected.as_ref().is_some_and(|rev| rev != &view.drev) {
        return Reply::err(format!("DIFFSTALE current={}", view.drev));
    }
    let cfg = engine.diff.config.clone();
    let g = stored.clone();
    let accepted = view.accepted.clone();
    let computed = engine
        .diff
        .pool
        .run(admission.clone(), move |cancel| {
            let mut base = None;
            for captured in inputs {
                cancel.check()?;
                let tree = snapshot::decode(&captured, &cfg.budget)?;
                if tree.sid == g.graph().from {
                    base = Some(tree);
                }
            }
            let base = base.ok_or_else(|| {
                marekvs_diff::DiffError::InvalidGraph("DIFFNOSNAPSHOT base missing".into())
            })?;
            match marekvs_diff::plan::plan_bounded(&base, g.graph(), &accepted, &cfg.budget, cancel)
            {
                Ok(plan) => {
                    let result = marekvs_diff::apply(&base, &plan);
                    let mut mapping = BTreeMap::new();
                    for (reference, id) in &plan.mapping {
                        if matches!(reference, marekvs_diff::graph::NodeRef::A(_)) {
                            let reference = serde_json::to_value(reference)
                                .expect("reference")
                                .as_str()
                                .unwrap()
                                .to_owned();
                            mapping.insert(reference, result.ordinal_path(*id));
                        }
                    }
                    Ok(Ok((result, mapping)))
                }
                Err(e) if !e.unresolved().is_empty() => Ok(Err(e.unresolved())),
                Err(e) => Err(marekvs_diff::DiffError::InvalidGraph(e.to_string())),
            }
        })
        .await;
    let (tree, mapping): (Tree, BTreeMap<String, Vec<u32>>) = match computed {
        Ok(Ok(v)) => v,
        Ok(Err(groups)) => return unresolved_reply(groups),
        Err(e) => return e,
    };
    let record = ResultRecord {
        gid: graph_name.clone(),
        drev: view.drev.clone(),
        sid: format!("{:032x}", tree.sid.0),
        mapping,
        at: view.decisions.values().map(|d| d.at).max().unwrap_or(0),
    };
    let encoded = serde_json::to_vec(&record).expect("result record");
    if encoded.len() > engine.diff.config.max_bytes {
        return Reply::err("DIFFTOOBIG result record");
    }
    let cfg = engine.diff.config.clone();
    let permit = admission.clone();
    let key = result_key;
    let publish_record = record.clone();
    let published = engine
        .store
        .run_key(&key.clone(), move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            if let Some(existing) = read_result(ctx, &key, &graph_name, cfg.max_bytes)? {
                return Ok(existing);
            }
            let current = decide::read_view(ctx, &tag, &stored, cfg.max_bytes)?;
            if current.drev != view.drev {
                return Err(Reply::err(format!("DIFFSTALE current={}", current.drev)));
            }
            _permit.cancel.check().map_err(super::error)?;
            snapshot::write_snapshot(ctx, &tag, &tree)?;
            compare::publish_string(ctx, &key, &encoded)?;
            Ok::<_, Reply>(publish_record)
        })
        .await;
    match published {
        Ok(r) => result_reply(&rid, &r),
        Err(e) => e,
    }
}

pub async fn merge3(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 4 {
        return Reply::wrong_args("diff.merge3");
    }
    let tag = match compare::same_tag(&[&args[1], &args[2], &args[3]]) {
        Ok(t) => t,
        Err(e) => return e,
    };
    let admission = match engine
        .diff
        .pool
        .admit(engine.diff.config.max_bytes.saturating_mul(3))
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    let _cancel = admission.cancel_on_drop();
    for key in &args[1..] {
        engine.ensure_local(key).await;
    }
    let keys = args[1..].to_vec();
    let cfg = engine.diff.config.clone();
    let permit = admission.clone();
    let captured = engine
        .store
        .run_key(&args[1], move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            keys.iter()
                .map(|key| snapshot::capture(ctx, key, &cfg))
                .collect::<Result<Vec<_>, Reply>>()
        })
        .await;
    let captured = match captured {
        Ok(v) => v,
        Err(e) => return e,
    };
    let e = engine.clone();
    let cfg = engine.diff.config.clone();
    let computed = engine
        .diff
        .pool
        .run(admission.clone(), move |cancel| {
            let trees = captured
                .iter()
                .map(|c| compare::decode_cached(c, &e))
                .collect::<Result<Vec<_>, _>>()?;
            let o = marekvs_diff::Options {
                budget: cfg.budget,
                cancel: cancel.clone(),
                ..Default::default()
            };
            let merged = marekvs_diff::merge3(&trees[0], &trees[1], &trees[2], &o)?;
            let mut stored = StoredGraph::Three(merged);
            stored.seal();
            Ok((trees, stored))
        })
        .await;
    let (trees, stored) = match computed {
        Ok(v) => v,
        Err(e) => return e,
    };
    let bytes = serde_json::to_vec(&stored).expect("merge graph");
    if bytes.len() > engine.diff.config.max_bytes {
        return Reply::err("DIFFTOOBIG merge graph metadata");
    }
    let event_tag = tag.clone();
    let permit = admission.clone();
    let key = engine
        .store
        .run_key(&args[1], move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            for t in trees {
                snapshot::write_snapshot(ctx, &tag, &t)?;
            }
            compare::publish_graph(ctx, &tag, &stored)
        })
        .await;
    // Defaults live in immutable merge metadata. No LWW initialization can
    // overwrite a later human decision on this node or a lagging publisher.
    match key {
        Ok(key) => {
            compare::publish_event(
                engine,
                &event_tag,
                serde_json::json!({"type":"graph","gkey":String::from_utf8_lossy(&key)}),
            );
            Reply::Array(vec![Reply::Bulk(key), Reply::Bulk(bytes)])
        }
        Err(e) => e,
    }
}
