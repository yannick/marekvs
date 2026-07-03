//! Pub/Sub commands. Subscribe confirmations are written straight into the
//! output buffer (they are multi-frame); message delivery happens through the
//! session's push channel in the server's connection loop.

use std::sync::Arc;

use crate::cmd::eq_ignore_case;
use crate::reply::Reply;
use crate::{Engine, Session};
use marekvs_resp::ReplyBuf;

fn confirm(out: &mut ReplyBuf, kind: &str, target: &[u8], count: usize) {
    out.push(3);
    out.bulk_str(kind);
    out.bulk(target);
    out.int(count as i64);
}

pub fn subscribe(
    engine: &Arc<Engine>,
    sess: &mut Session,
    args: &[Vec<u8>],
    out: &mut ReplyBuf,
) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("subscribe");
    }
    for ch in &args[1..] {
        if !sess.subs.contains(ch) {
            engine.pubsub.subscribe(sess.id, ch, sess.push_tx.clone());
            sess.subs.push(ch.clone());
        }
        confirm(out, "subscribe", ch, sess.sub_count());
    }
    Reply::None
}

pub fn unsubscribe(
    engine: &Arc<Engine>,
    sess: &mut Session,
    args: &[Vec<u8>],
    out: &mut ReplyBuf,
) -> Reply {
    let targets: Vec<Vec<u8>> = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        sess.subs.clone()
    };
    if targets.is_empty() {
        confirm(out, "unsubscribe", b"", 0);
        return Reply::None;
    }
    for ch in targets {
        engine.pubsub.unsubscribe(sess.id, &ch);
        sess.subs.retain(|c| c != &ch);
        confirm(out, "unsubscribe", &ch, sess.sub_count());
    }
    Reply::None
}

pub fn psubscribe(
    engine: &Arc<Engine>,
    sess: &mut Session,
    args: &[Vec<u8>],
    out: &mut ReplyBuf,
) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("psubscribe");
    }
    for pat in &args[1..] {
        if !sess.psubs.contains(pat) {
            engine.pubsub.psubscribe(sess.id, pat, sess.push_tx.clone());
            sess.psubs.push(pat.clone());
        }
        confirm(out, "psubscribe", pat, sess.sub_count());
    }
    Reply::None
}

pub fn punsubscribe(
    engine: &Arc<Engine>,
    sess: &mut Session,
    args: &[Vec<u8>],
    out: &mut ReplyBuf,
) -> Reply {
    let targets: Vec<Vec<u8>> = if args.len() > 1 {
        args[1..].to_vec()
    } else {
        sess.psubs.clone()
    };
    if targets.is_empty() {
        confirm(out, "punsubscribe", b"", 0);
        return Reply::None;
    }
    for pat in targets {
        engine.pubsub.punsubscribe(sess.id, &pat);
        sess.psubs.retain(|p| p != &pat);
        confirm(out, "punsubscribe", &pat, sess.sub_count());
    }
    Reply::None
}

pub fn publish(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() != 3 {
        return Reply::wrong_args("publish");
    }
    Reply::Int(engine.pubsub.publish(&args[1], &args[2]) as i64)
}

pub fn pubsub_cmd(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("pubsub");
    }
    if eq_ignore_case(&args[1], "CHANNELS") {
        let channels = engine
            .pubsub
            .channels_matching(args.get(2).map(|v| v.as_slice()));
        Reply::Array(channels.into_iter().map(Reply::Bulk).collect())
    } else if eq_ignore_case(&args[1], "NUMSUB") {
        let mut out = Vec::new();
        for ch in &args[2..] {
            out.push(Reply::Bulk(ch.clone()));
            out.push(Reply::Int(engine.pubsub.numsub(ch) as i64));
        }
        Reply::Array(out)
    } else if eq_ignore_case(&args[1], "NUMPAT") {
        Reply::Int(engine.pubsub.numpat() as i64)
    } else {
        Reply::syntax()
    }
}
