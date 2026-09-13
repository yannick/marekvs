//! Working-copy publication, retaining the source's RGA identities.
use super::{
    keys::DiffKey,
    snapshot::{self, Captured},
};
use crate::{
    cmd::generic,
    reply::Reply,
    store::{self, ShardCtx},
    Engine,
};
use marekvs_core::{
    envelope::{head, Envelope, RecordType},
    ikey,
    json::NodeIn,
    merge::{element_dots, element_set},
};
use std::sync::Arc;

/// Replace a branch from a previously captured record set. Caller validates
/// source/destination revisions and destination authorization first.
pub(crate) fn write_branch(ctx: &ShardCtx, key: &[u8], source: &Captured) -> Result<(), Reply> {
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
    del = del.max(generic::del_key_hlc(ctx, key).unwrap_or(0));
    // Also fence physically present records with no published head (failed
    // publication), and stale records under a previously emptied branch.
    let fence = ctx.hlc.now().max(del.saturating_add(1));
    del = del.max(fence);
    ctx.hlc.observe(del);
    for (path, node) in &source.nodes {
        let rk = ikey::json_node_key(key, path);
        let hlc = ctx.hlc.now().max(del.saturating_add(1));
        let rec = match node {
            NodeIn::Map { val, .. } => {
                let observed = store::get_raw(ctx, &rk)
                    .and_then(|v| Envelope::decode(&v).map(|(_, p)| element_dots(p)))
                    .unwrap_or_default();
                element_set(
                    RecordType::HashField,
                    hlc,
                    ctx.node_id,
                    &val.encode(),
                    &observed,
                )
            }
            NodeIn::ArrElem { elem, live } => {
                let env = if *live {
                    Envelope::new(RecordType::List, hlc, ctx.node_id)
                } else {
                    Envelope::tombstone(RecordType::List, hlc, ctx.node_id)
                };
                env.encode_with(&elem.encode())
            }
        };
        store::write_merged(ctx, &rk, &rec);
    }
    let rec = Envelope::head(ctx.hlc.now().max(del.saturating_add(1)), ctx.node_id)
        .encode_with(&head::encode(head::CTYPE_JSON, del));
    store::write_merged(ctx, &ikey::head_key(key), &rec);
    Ok(())
}
pub async fn fork(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if !(args.len() == 3 || args.len() == 4 && args[3].eq_ignore_ascii_case(b"REPLACE")) {
        return Reply::wrong_args("diff.fork");
    }
    let src = match DiffKey::parse(&args[1]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let dst = match DiffKey::parse(&args[2]) {
        Ok(v) => v,
        Err(e) => return e,
    };
    if src.tag() != dst.tag() {
        return Reply::err("CROSSSLOT DIFF keys must share one tag");
    }
    if !matches!(dst, DiffKey::Branch { .. }) {
        return Reply::err("DIFFKEY destination must be a branch");
    }
    if args[1] == args[2] {
        return Reply::err("DIFFKEY source and destination must differ");
    }
    let config = engine.diff.config.clone();
    let admission = match engine.diff.pool.admit(config.max_bytes) {
        Ok(v) => v,
        Err(e) => return e,
    };
    let _cancel = admission.cancel_on_drop();
    engine.ensure_local(&args[1]).await;
    engine.ensure_local(&args[2]).await;
    let source = args[1].clone();
    let dest = args[2].clone();
    let replace = args.len() == 4;
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
            if !replace && store::key_type(ctx, &dest).is_some() {
                return Err(Reply::err("DIFFEXISTS destination exists; use REPLACE"));
            }
            snapshot::capture(ctx, &source, &scan_config).and_then(|c| {
                snapshot::observe_records(&scan_metrics, &c);
                snapshot::revision(ctx, &dest, &scan_config).map(|r| (c, r))
            })
        })
        .await
    {
        Ok(v) => v,
        Err(e) => return e,
    };
    let (captured, dest_revision) = captured;
    let input = captured.clone();
    let budget = config.budget.clone();
    if let Err(e) = engine
        .diff
        .pool
        .run(admission.clone(), move |cancel| {
            cancel.check()?;
            snapshot::decode(&input, &budget).map(|_| ())
        })
        .await
    {
        return e;
    }
    let dest = args[2].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            let _guard = admission;
            if let Err(e) = _guard.cancel.check() {
                return super::error(e);
            }
            if let Err(e) = snapshot::verify_revision(ctx, &captured, &config) {
                return e;
            }
            if !replace && store::key_type(ctx, &dest).is_some() {
                return Reply::err("DIFFEXISTS destination exists; use REPLACE");
            }
            if snapshot::revision(ctx, &dest, &config).ok() != Some(dest_revision) {
                return Reply::err("DIFFSTALE destination changed");
            }
            if let Err(e) = _guard.cancel.check() {
                return super::error(e);
            }
            write_branch(ctx, &dest, &captured)
                .map(|_| Reply::ok())
                .unwrap_or_else(|e| e)
        })
        .await
}
