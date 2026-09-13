//! Comparison, immutable graph publication and shared graph readers.
use super::{keys::DiffKey, snapshot};
use crate::{
    reply::Reply,
    store::{self, ShardCtx},
    Engine,
};
use marekvs_core::{envelope::RecordType, ikey};
use marekvs_diff::{Graph, Level, MergeGraph, Options, Sid, Tree};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StoredGraph {
    Three(MergeGraph),
    Two(Graph),
}
impl StoredGraph {
    pub fn seal(&mut self) {
        match self {
            Self::Two(g) => g.rehash(),
            Self::Three(m) => {
                m.graph.gid.0.clear();
                let mut bytes = b"marekvs-diff/stored-merge/v2".to_vec();
                bytes.extend(serde_json::to_vec(m).expect("merge serialization"));
                m.graph.gid.0 = format!("{:032x}", xxhash_rust::xxh3::xxh3_128(&bytes));
            }
        }
    }
    pub fn graph(&self) -> &Graph {
        match self {
            Self::Two(g) => g,
            Self::Three(m) => &m.graph,
        }
    }
    pub fn inputs(&self) -> Vec<Sid> {
        match self {
            Self::Two(g) => vec![g.from, g.to],
            Self::Three(m) => vec![m.base, m.left, m.right],
        }
    }
    pub fn default_accepts(&self) -> marekvs_diff::Accepted {
        match self {
            Self::Two(_) => marekvs_diff::Accepted::none(),
            Self::Three(m) => m
                .graph
                .changes
                .iter()
                .filter(|c| !m.conflicts.iter().any(|g| g.contains(&c.id)))
                .map(|c| c.id.clone())
                .collect(),
        }
    }
}

pub fn document_tag(key: &[u8]) -> Result<String, Reply> {
    let parsed = DiffKey::parse(key)?;
    if !matches!(parsed, DiffKey::Branch { .. } | DiffKey::Snapshot { .. }) {
        return Err(Reply::err(
            "DIFFKEY expected document branch or snapshot key",
        ));
    }
    Ok(parsed.tag().to_owned())
}
pub fn graph_tag(key: &[u8]) -> Result<String, Reply> {
    let parsed = DiffKey::parse(key)?;
    if !matches!(parsed, DiffKey::Graph { .. }) {
        return Err(Reply::err("DIFFKEY expected full graph key"));
    }
    Ok(parsed.tag().to_owned())
}
pub fn same_tag(keys: &[&[u8]]) -> Result<String, Reply> {
    let first = document_tag(keys.first().ok_or_else(Reply::syntax)?)?;
    for key in keys.iter().skip(1) {
        if document_tag(key)? != first {
            return Err(Reply::err("CROSSSLOT DIFF keys must share a hash tag"));
        }
    }
    Ok(first)
}
pub fn options(
    args: &[Vec<u8>],
    start: usize,
    engine: &Engine,
    theta: f32,
) -> Result<Options, Reply> {
    let mut o = Options {
        theta,
        budget: engine.diff.config.budget.clone(),
        ..Default::default()
    };
    let mut i = start;
    let mut seen = std::collections::BTreeSet::new();
    while i < args.len() {
        let key = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        if !seen.insert(key.clone()) || i + 1 >= args.len() {
            return Err(Reply::syntax());
        }
        match key.as_str() {
            "THETA" => {
                o.theta = std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(Reply::not_float)?
            }
            "LEVEL" => {
                o.level = match String::from_utf8_lossy(&args[i + 1])
                    .to_ascii_lowercase()
                    .as_str()
                {
                    "sen" => Level::Sen,
                    "word" => Level::Word,
                    "char" => Level::Char,
                    _ => return Err(Reply::syntax()),
                }
            }
            _ => return Err(Reply::syntax()),
        }
        i += 2;
    }
    o.validate().map_err(super::error)?;
    Ok(o)
}
pub fn publish_graph(ctx: &ShardCtx, tag: &str, stored: &StoredGraph) -> Result<Vec<u8>, Reply> {
    let key = format!("diff:{{{tag}}}:g:{}", stored.graph().gid.0).into_bytes();
    let bytes = serde_json::to_vec(stored).map_err(|e| Reply::err(format!("DIFFINVALID {e}")))?;
    publish_string(ctx, &key, &bytes)?;
    Ok(key)
}
pub fn publish_string(ctx: &ShardCtx, key: &[u8], bytes: &[u8]) -> Result<(), Reply> {
    if let Some((_, old)) = store::read_lww(ctx, &ikey::string_key(key), 0) {
        return if old == bytes {
            Ok(())
        } else {
            Err(Reply::err("DIFFCOLLISION immutable artifact differs"))
        };
    }
    if store::key_type(ctx, key).is_some() {
        return Err(Reply::wrongtype());
    }
    store::write_merged(
        ctx,
        &ikey::string_key(key),
        &store::new_lww(ctx, RecordType::String, bytes, 0),
    );
    if !store::read_lww(ctx, &ikey::string_key(key), 0).is_some_and(|(_, actual)| actual == bytes) {
        return Err(Reply::err("DIFFWRITE artifact publication failed"));
    }
    Ok(())
}
pub fn read_graph(ctx: &ShardCtx, key: &[u8], max_bytes: usize) -> Result<StoredGraph, Reply> {
    graph_tag(key)?;
    let (_, bytes) = store::read_lww(ctx, &ikey::string_key(key), 0)
        .ok_or_else(|| Reply::err("DIFFNOGRAPH graph missing"))?;
    if bytes.len() > max_bytes {
        return Err(Reply::err("DIFFTOOBIG graph bytes"));
    }
    let stored: StoredGraph = serde_json::from_slice(&bytes)
        .map_err(|_| Reply::err("DIFFINVALID malformed stored graph"))?;
    let mut check = stored.clone();
    let original = stored.graph().gid.clone();
    check.seal();
    if check.graph().gid != original || !key.ends_with(original.0.as_bytes()) {
        return Err(Reply::err("DIFFINVALID graph digest mismatch"));
    }
    Ok(stored)
}
pub fn publish_event(engine: &Engine, tag: &str, event: serde_json::Value) {
    engine.pubsub.publish(
        format!("diff:{{{tag}}}:events").as_bytes(),
        &serde_json::to_vec(&event).expect("event serialization"),
    );
}
pub(crate) fn decode_cached(
    capture: &snapshot::Captured,
    engine: &Engine,
) -> Result<Tree, marekvs_diff::DiffError> {
    engine
        .diff
        .metrics
        .records
        .with_label_values(&["physical"])
        .inc_by(capture.physical as u64);
    engine
        .diff
        .metrics
        .records
        .with_label_values(&["live"])
        .inc_by(capture.live as u64);
    let parsed = snapshot::decode(capture, &engine.diff.config.budget)?;
    if let Some(cached) = engine.diff.cache.get(parsed.sid) {
        let mut tree = (*cached).clone();
        for (node, source) in tree.nodes.iter_mut().zip(&parsed.nodes) {
            node.eid = source.eid;
        }
        return Ok(tree);
    }
    let mut semantic = parsed.clone();
    for n in &mut semantic.nodes {
        n.eid = None;
    }
    engine.diff.cache.put(parsed.sid, Arc::new(semantic));
    Ok(parsed)
}

pub async fn compare(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("diff.compare");
    }
    let tag = match same_tag(&[&args[1], &args[2]]) {
        Ok(t) => t,
        Err(e) => return e,
    };
    let mut o = match options(args, 3, engine, 0.5) {
        Ok(o) => o,
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
    engine.ensure_local(&args[2]).await;
    let keys = [args[1].clone(), args[2].clone()];
    let route = keys[0].clone();
    let cfg = engine.diff.config.clone();
    let permit = admission.clone();
    let captures = engine
        .store
        .run_key(&route, move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            Ok::<_, Reply>((
                snapshot::capture(ctx, &keys[0], &cfg)?,
                snapshot::capture(ctx, &keys[1], &cfg)?,
            ))
        })
        .await;
    let (a, b) = match captures {
        Ok(c) => c,
        Err(e) => return e,
    };
    let e = engine.clone();
    let result = engine
        .diff
        .pool
        .run(admission.clone(), move |cancel| {
            o.cancel = cancel.clone();
            let a = decode_cached(&a, &e)?;
            let b = decode_cached(&b, &e)?;
            let g = marekvs_diff::diff(&a, &b, &o)?;
            Ok((a, b, StoredGraph::Two(g)))
        })
        .await;
    let (a, b, stored) = match result {
        Ok(v) => v,
        Err(e) => return e,
    };
    let json = serde_json::to_vec(&stored).expect("graph serialization");
    let event_tag = tag.clone();
    let permit = admission.clone();
    let result = engine
        .store
        .run_key(&route, move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            snapshot::write_snapshot(ctx, &tag, &a)?;
            snapshot::write_snapshot(ctx, &tag, &b)?;
            publish_graph(ctx, &tag, &stored)
        })
        .await;
    match result {
        Ok(key) => {
            publish_event(
                engine,
                &event_tag,
                serde_json::json!({"type":"graph","gkey":String::from_utf8_lossy(&key)}),
            );
            Reply::Array(vec![Reply::Bulk(key), Reply::Bulk(json)])
        }
        Err(e) => e,
    }
}

pub async fn hash(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if !(2..=3).contains(&args.len()) {
        return Reply::wrong_args("diff.hash");
    }
    if let Err(e) = document_tag(&args[1]) {
        return e;
    }
    let admission = match engine.diff.pool.admit(engine.diff.config.max_bytes) {
        Ok(a) => a,
        Err(e) => return e,
    };
    let _cancel = admission.cancel_on_drop();
    engine.ensure_local(&args[1]).await;
    let key = args[1].clone();
    let cfg = engine.diff.config.clone();
    let permit = admission.clone();
    let captured = engine
        .store
        .run_key(&key.clone(), move |ctx| {
            let _permit = permit;
            _permit.cancel.check().map_err(super::error)?;
            snapshot::capture(ctx, &key, &cfg)
        })
        .await;
    let captured = match captured {
        Ok(c) => c,
        Err(e) => return e,
    };
    let e = engine.clone();
    let path = args.get(2).cloned();
    match engine
        .diff
        .pool
        .run(admission, move |_| {
            let tree = decode_cached(&captured, &e)?;
            let mut id = tree.root;
            if let Some(path) = path {
                let s = std::str::from_utf8(&path)
                    .map_err(|_| marekvs_diff::DiffError::InvalidGraph("invalid path".into()))?;
                if s != "$" {
                    let mut tail = s.strip_prefix('$').ok_or_else(|| {
                        marekvs_diff::DiffError::InvalidGraph("path must be $.c[index]...".into())
                    })?;
                    while !tail.is_empty() {
                        tail = tail.strip_prefix(".c[").ok_or_else(|| {
                            marekvs_diff::DiffError::InvalidGraph(
                                "path must be $.c[index]...".into(),
                            )
                        })?;
                        let end = tail.find(']').ok_or_else(|| {
                            marekvs_diff::DiffError::InvalidGraph("invalid path".into())
                        })?;
                        let ordinal: usize = tail[..end].parse().map_err(|_| {
                            marekvs_diff::DiffError::InvalidGraph("invalid ordinal".into())
                        })?;
                        id = *tree.children(id).get(ordinal).ok_or_else(|| {
                            marekvs_diff::DiffError::InvalidGraph("path missing".into())
                        })?;
                        tail = &tail[end + 1..];
                    }
                }
            }
            let n = tree.node(id);
            Ok((n.h_exact, n.h_content, n.weight))
        })
        .await
    {
        Ok((exact, content, weight)) => Reply::Map(vec![
            (
                Reply::bulk_str("exact"),
                Reply::bulk_str(format!("{exact:032x}")),
            ),
            (
                Reply::bulk_str("content"),
                Reply::bulk_str(format!("{content:032x}")),
            ),
            (Reply::bulk_str("weight"), Reply::Int(weight.into())),
        ]),
        Err(e) => e,
    }
}
pub fn stats(engine: &Engine, args: &[Vec<u8>]) -> Reply {
    if args.len() != 1 {
        return Reply::wrong_args("diff.stats");
    }
    engine.diff.stats()
}
