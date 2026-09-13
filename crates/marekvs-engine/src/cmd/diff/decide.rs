//! Atomic scalar decisions, default merge decisions and revision views.
use super::{
    compare::{self, StoredGraph},
    snapshot,
};
use crate::{
    reply::Reply,
    store::{self, ShardCtx},
    Engine,
};
use marekvs_core::{
    envelope::{head, Envelope, RecordType},
    ikey,
    json::{self, JVal, Seg},
    merge::{element_dots, element_set, merge_values, resolve, ElementState},
};
use marekvs_diff::{Accepted, ChangeId};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum State {
    Accepted,
    Rejected,
    Pending,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Decision {
    pub state: State,
    pub by: String,
    pub at: u64,
}
#[derive(Clone, Debug)]
pub struct DecisionView {
    pub decisions: BTreeMap<String, Decision>,
    pub drev: String,
    pub accepted: Accepted,
    pub raw_bytes: usize,
}
pub fn decision_key(tag: &str, stored: &StoredGraph) -> Vec<u8> {
    format!("diff:{{{tag}}}:d:{}", stored.graph().gid.0).into_bytes()
}
fn decision_path(id: &str) -> Vec<u8> {
    json::encode_path(&[Seg::Field(id.as_bytes().to_vec())])
}

pub fn read_view(
    ctx: &ShardCtx,
    tag: &str,
    stored: &StoredGraph,
    max_bytes: usize,
) -> Result<DecisionView, Reply> {
    let key = decision_key(tag, stored);
    let defaults = stored.default_accepts();
    let root_record = store::get_raw(ctx, &ikey::json_node_key(&key, &[]));
    let mut bytes = root_record.as_ref().map_or(0, Vec::len);
    if bytes > max_bytes {
        return Err(Reply::err("DIFFTOOBIG decisions bytes"));
    }
    store::check_type(ctx, &key, head::CTYPE_JSON).map_err(|_| Reply::wrongtype())?;
    let del = match store::get_head(ctx, &key) {
        Some((env, ctype, del)) if !env.is_tombstone() && !env.is_expired(store::now_ms()) => {
            if ctype != head::CTYPE_JSON {
                return Err(Reply::wrongtype());
            }
            let root = root_record.as_ref().and_then(|raw| {
                let (root_env, payload) = Envelope::decode(raw)?;
                if root_env.rtype() != RecordType::HashField
                    || root_env.hlc <= del
                    || root_env.is_expired(store::now_ms())
                    || root_env.is_tombstone()
                {
                    return None;
                }
                let state = ElementState::decode(payload)?;
                JVal::decode(state.value()?)
            });
            if root != Some(JVal::Obj) {
                return Err(Reply::err("DIFFDECISIONS incomplete decision document"));
            }
            Some(del)
        }
        _ => {
            if store::key_type(ctx, &key).is_some() {
                return Err(Reply::wrongtype());
            }
            None
        }
    };
    let mut decisions = BTreeMap::new();
    let mut accepted = Accepted::none();
    for change in &stored.graph().changes {
        let id = &change.id.0;
        let record = store::get_raw(ctx, &ikey::json_node_key(&key, &decision_path(id)));
        bytes = bytes.saturating_add(record.as_ref().map_or(0, Vec::len));
        if bytes > max_bytes {
            return Err(Reply::err("DIFFTOOBIG decisions bytes"));
        }
        let raw = if let (Some(record), Some(del)) = (record.as_ref(), del) {
            let (env, payload) = Envelope::decode(record)
                .ok_or_else(|| Reply::err("DIFFDECISIONS malformed decision envelope"))?;
            if env.hlc <= del || env.is_expired(store::now_ms()) || env.is_tombstone() {
                None
            } else {
                if env.rtype() != RecordType::HashField {
                    return Err(Reply::err("DIFFDECISIONS invalid decision record type"));
                }
                let state = ElementState::decode(payload)
                    .ok_or_else(|| Reply::err("DIFFDECISIONS malformed decision record"))?;
                state.value().map(<[u8]>::to_vec)
            }
        } else {
            None
        };
        let decision = if let Some(raw) = raw {
            let Some(JVal::Str(encoded)) = JVal::decode(&raw) else {
                return Err(Reply::err("DIFFDECISIONS invalid scalar decision"));
            };
            let (state, by, at): (State, String, u64) = serde_json::from_slice(&encoded)
                .map_err(|_| Reply::err("DIFFDECISIONS malformed decision"))?;
            Decision { state, by, at }
        } else {
            Decision {
                state: if defaults.contains(&change.id) {
                    State::Accepted
                } else {
                    State::Pending
                },
                by: "-".into(),
                at: 0,
            }
        };
        if decision.state == State::Accepted {
            accepted.0.insert(change.id.clone());
        }
        decisions.insert(id.clone(), decision);
    }
    let drev = format!(
        "{:032x}",
        xxhash_rust::xxh3::xxh3_128(&serde_json::to_vec(&decisions).expect("decisions serialize"))
    );
    Ok(DecisionView {
        decisions,
        drev,
        accepted,
        raw_bytes: bytes,
    })
}
pub async fn decide(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 4 {
        return Reply::wrong_args("diff.decide");
    }
    let tag = match compare::graph_tag(&args[1]) {
        Ok(t) => t,
        Err(e) => return e,
    };
    let mut i = 2;
    let mut by = "-".to_owned();
    if args.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"BY")) {
        let Some(principal) = args.get(i + 1) else {
            return Reply::syntax();
        };
        if principal.len() > 1024 {
            return Reply::err("DIFFPRINCIPAL oversized principal");
        }
        by = match String::from_utf8(principal.clone()) {
            Ok(s) if s.len() <= 1024 => s,
            _ => return Reply::err("DIFFPRINCIPAL invalid or oversized principal"),
        };
        i += 2;
    }
    if i >= args.len() || !(args.len() - i).is_multiple_of(2) {
        return Reply::wrong_args("diff.decide");
    }
    if (args.len() - i) / 2 > engine.diff.config.budget.max_changes {
        return Reply::err("DIFFTOOBIG decisions count");
    }
    let mut writes = Vec::new();
    let mut seen = BTreeSet::new();
    for pair in args[i..].chunks_exact(2) {
        if pair[0].len() != 34
            || !pair[0].starts_with(b"c:")
            || !pair[0][2..]
                .iter()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
        {
            return Reply::err("DIFFCHANGE invalid change id");
        }
        let id = match String::from_utf8(pair[0].clone()) {
            Ok(s) => s,
            Err(_) => return Reply::syntax(),
        };
        if !seen.insert(id.clone()) {
            return Reply::err("DIFFCHANGE duplicate change id");
        }
        let state = match pair[1].as_slice() {
            b"accept" => State::Accepted,
            b"reject" => State::Rejected,
            b"pending" => State::Pending,
            _ => return Reply::syntax(),
        };
        writes.push((id, state));
    }
    if writes.len() > engine.diff.config.budget.max_changes {
        return Reply::err("DIFFTOOBIG decisions count");
    }
    let admission = match engine.diff.pool.admit(engine.diff.config.max_bytes) {
        Ok(a) => a,
        Err(e) => return e,
    };
    let _cancel = admission.cancel_on_drop();
    engine.ensure_local(&args[1]).await;
    let dkey = format!(
        "diff:{{{tag}}}:d:{}",
        String::from_utf8_lossy(&args[1])
            .rsplit(':')
            .next()
            .unwrap()
    )
    .into_bytes();
    engine.ensure_local(&dkey).await;
    let graph_key = args[1].clone();
    let cfg = engine.diff.config.clone();
    let event_tag = tag.clone();
    let event_key = String::from_utf8_lossy(&graph_key).into_owned();
    let ids = seen.into_iter().collect::<Vec<_>>();
    let permit = admission.clone();
    let result = engine
        .store
        .run_key(&graph_key.clone(), move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            let stored = compare::read_graph(ctx, &graph_key, cfg.max_bytes)?;
            let valid: BTreeSet<_> = stored
                .graph()
                .changes
                .iter()
                .map(|c| c.id.0.as_str())
                .collect();
            if writes.iter().any(|(id, _)| !valid.contains(id.as_str())) {
                return Err(Reply::err("DIFFCHANGE unknown change id"));
            }
            let view = read_view(ctx, &tag, &stored, cfg.max_bytes)?;
            let mut encoded = Vec::new();
            let mut total = 0usize;
            let at = store::now_ms();
            for (id, state) in writes {
                let value = serde_json::to_vec(&(state, &by, at)).expect("scalar decision");
                total = total.saturating_add(value.len());
                if total > cfg.max_bytes {
                    return Err(Reply::err("DIFFTOOBIG decision batch bytes"));
                }
                encoded.push((id, JVal::Str(value)));
            }
            let key = decision_key(&tag, &stored);
            let existing = store::get_head(ctx, &key);
            let active = existing
                .as_ref()
                .is_some_and(|(e, _, _)| !e.is_tombstone() && !e.is_expired(at));
            let del = if active {
                existing.as_ref().unwrap().2
            } else if existing.is_some() {
                ctx.hlc.now()
            } else {
                0
            };
            let mut projected = view.raw_bytes;
            let mut records = Vec::new();
            if !active {
                let root_key = ikey::json_node_key(&key, &[]);
                let local = store::get_raw(ctx, &root_key).unwrap_or_default();
                let incoming = element_set(
                    RecordType::HashField,
                    ctx.hlc.now(),
                    ctx.node_id,
                    &JVal::Obj.encode(),
                    &[],
                );
                let merged = merge_values(&local, &incoming);
                projected = projected
                    .saturating_sub(local.len())
                    .saturating_add(resolve(&local, &incoming, &merged).len());
                if projected > cfg.max_bytes {
                    return Err(Reply::err("DIFFTOOBIG decisions bytes"));
                }
                records.push((root_key, incoming));
            }
            for (id, value) in encoded {
                let ikey = ikey::json_node_key(&key, &decision_path(&id));
                let local = store::get_raw(ctx, &ikey).unwrap_or_default();
                let observed = Envelope::decode(&local)
                    .filter(|(env, _)| env.hlc > del && !env.is_expired(at) && !env.is_tombstone())
                    .map(|(_, pay)| element_dots(pay))
                    .unwrap_or_default();
                let incoming = element_set(
                    RecordType::HashField,
                    ctx.hlc.now(),
                    ctx.node_id,
                    &value.encode(),
                    &observed,
                );
                let merged = merge_values(&local, &incoming);
                projected = projected
                    .saturating_sub(local.len())
                    .saturating_add(resolve(&local, &incoming, &merged).len());
                if projected > cfg.max_bytes {
                    return Err(Reply::err("DIFFTOOBIG decisions bytes"));
                }
                records.push((ikey, incoming));
            }
            _permit.cancel.check().map_err(super::error)?;
            for (ikey, incoming) in records {
                store::write_merged(ctx, &ikey, &incoming);
            }
            if !active {
                let h = Envelope::head(ctx.hlc.now(), ctx.node_id);
                store::write_merged(
                    ctx,
                    &ikey::head_key(&key),
                    &h.encode_with(&head::encode(head::CTYPE_JSON, del)),
                );
            }
            Ok::<_, Reply>(())
        })
        .await;
    match result {
        Ok(()) => {
            compare::publish_event(
                engine,
                &event_tag,
                serde_json::json!({"type":"decisions","gkey":event_key,"ids":ids}),
            );
            Reply::ok()
        }
        Err(e) => e,
    }
}
pub fn view_reply(view: &DecisionView, unresolved: Vec<Vec<ChangeId>>) -> Reply {
    Reply::Map(vec![
        (
            Reply::bulk_str("decisions"),
            Reply::Bulk(serde_json::to_vec(&view.decisions).expect("view JSON")),
        ),
        (Reply::bulk_str("drev"), Reply::bulk_str(&view.drev)),
        (
            Reply::bulk_str("unresolved"),
            Reply::Array(
                unresolved
                    .into_iter()
                    .map(|g| Reply::Array(g.into_iter().map(|id| Reply::bulk_str(id.0)).collect()))
                    .collect(),
            ),
        ),
    ])
}
pub async fn decisions(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 2 {
        return Reply::wrong_args("diff.decisions");
    }
    let tag = match compare::graph_tag(&args[1]) {
        Ok(t) => t,
        Err(e) => return e,
    };
    let admission = match engine
        .diff
        .pool
        .admit(engine.diff.config.max_bytes.saturating_mul(2))
    {
        Ok(a) => a,
        Err(e) => return e,
    };
    let _cancel = admission.cancel_on_drop();
    engine.ensure_local(&args[1]).await;
    let key = args[1].clone();
    let cfg = engine.diff.config.clone();
    let permit = admission.clone();
    let loaded = engine
        .store
        .run_key(&key.clone(), move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            let g = compare::read_graph(ctx, &key, cfg.max_bytes)?;
            Ok::<_, Reply>((g, tag))
        })
        .await;
    let (stored, tag) = match loaded {
        Ok(v) => v,
        Err(e) => return e,
    };
    let base_key = super::keys::snapshot_key(&tag, stored.graph().from);
    let dkey = decision_key(&tag, &stored);
    engine.ensure_local(&base_key).await;
    engine.ensure_local(&dkey).await;
    let cfg = engine.diff.config.clone();
    let permit = admission.clone();
    let captured = engine
        .store
        .run_key(&base_key.clone(), move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            let base = snapshot::capture(ctx, &base_key, &cfg)?;
            let view = read_view(ctx, &tag, &stored, cfg.max_bytes)?;
            Ok::<_, Reply>((base, stored, view))
        })
        .await;
    let (base, stored, view) = match captured {
        Ok(v) => v,
        Err(e) => return e,
    };
    let cfg = engine.diff.config.clone();
    let reply_view = view.clone();
    match engine
        .diff
        .pool
        .run(admission, move |cancel| {
            let tree = snapshot::decode(&base, &cfg.budget)?;
            match marekvs_diff::plan::plan_bounded(
                &tree,
                stored.graph(),
                &view.accepted,
                &cfg.budget,
                cancel,
            ) {
                Ok(_) => Ok(Vec::new()),
                Err(e) if !e.unresolved().is_empty() => Ok(e.unresolved()),
                Err(e) => Err(marekvs_diff::DiffError::InvalidGraph(e.to_string())),
            }
        })
        .await
    {
        Ok(groups) => view_reply(&reply_view, groups),
        Err(e) => e,
    }
}
