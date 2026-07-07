//! Command dispatch: one match arm per command, grouped in family modules

pub mod budget;
pub mod cluster;
pub mod command_docs;
pub mod generic;
pub mod hash;
pub mod hll;
pub mod json;
pub mod list;
pub mod proto;
pub mod pubsub;
pub mod script;
pub mod server;
pub mod set;
pub mod stream;
pub mod string;
pub mod zset;

use std::sync::Arc;

use crate::reply::Reply;
use crate::{Engine, Session};
use marekvs_resp::ReplyBuf;

pub async fn dispatch(
    engine: &Arc<Engine>,
    sess: &mut Session,
    name: &str,
    args: Vec<Vec<u8>>,
    out: &mut ReplyBuf,
) -> Reply {
    // Disk high-water guard: refuse client writes cleanly instead of letting
    // ondadb hit ENOSPC mid-compaction (which wedges the node). Sits here so
    // it also covers the EXEC loop; internal sessions (REPLICAOF apply) are
    // exempt — refusing merges would silently diverge a follower.
    if !sess.internal
        && engine
            .write_stopped
            .load(std::sync::atomic::Ordering::Relaxed)
        && Engine::is_write_command(name)
    {
        return Reply::err(
            "MISCONF disk usage above high-water mark; write commands are rejected \
             until space is freed (see marekvs_disk_* metrics)",
        );
    }
    match name {
        // --- connection / server ---
        "PING" => server::ping(&args),
        "ECHO" => server::echo(&args),
        "HELLO" => server::hello(engine, sess, &args, out),
        "AUTH" => server::auth(engine, sess, &args),
        "QUIT" => {
            sess.should_close = true;
            Reply::ok()
        }
        "RESET" => server::reset(engine, sess),
        "SELECT" => server::select(&args),
        "CLIENT" => server::client(sess, &args),
        "COMMAND" => server::command_cmd(&args),
        "CONFIG" => server::config(engine, &args),
        "INFO" => server::info(engine, &args).await,
        "DBSIZE" => server::dbsize(engine).await,
        "FLUSHALL" | "FLUSHDB" => server::flushall(engine).await,
        "TIME" => server::time(),
        "REPLICAOF" | "SLAVEOF" => server::replicaof(engine, &args),
        "SHUTDOWN" => {
            sess.should_close = true;
            std::process::exit(0)
        }
        "DEBUG" => server::debug(engine, &args).await,

        // --- cluster topology, read-only (design/15) ---
        "CLUSTER" => cluster::cluster(engine, sess, &args),

        // --- budgets (BG.*, design/13) ---
        "BG.CREATE" => budget::create(engine, &args).await,
        "BG.TOPUP" => budget::topup(engine, &args).await,
        "BG.RESERVE" => budget::reserve(engine, &args).await,
        "BG.COMMIT" => budget::commit(engine, &args).await,
        "BG.RELEASE" => budget::release(engine, &args).await,
        "BG.DRAW" => budget::draw(engine, &args).await,
        "BG.INFO" => budget::info(engine, &args).await,
        "BG.RECLAIM" => budget::reclaim(engine, &args).await,

        // --- JSON documents (JSON.*, design/16) ---
        "JSON.SET" => json::set(engine, &args).await,
        "JSON.GET" => json::get(engine, &args).await,
        "JSON.MGET" => json::mget(engine, &args).await,
        "JSON.MSET" => json::mset(engine, &args).await,
        "JSON.DEL" | "JSON.FORGET" => json::del(engine, &args).await,
        "JSON.TYPE" => json::type_cmd(engine, &args).await,
        "JSON.NUMINCRBY" => json::numop(engine, &args, false).await,
        "JSON.NUMMULTBY" => json::numop(engine, &args, true).await,
        "JSON.STRAPPEND" => json::strappend(engine, &args).await,
        "JSON.STRLEN" => json::strlen(engine, &args).await,
        "JSON.ARRAPPEND" => json::arrappend(engine, &args).await,
        "JSON.ARRINDEX" => json::arrindex(engine, &args).await,
        "JSON.ARRINSERT" => json::arrinsert(engine, &args).await,
        "JSON.ARRLEN" => json::arrlen(engine, &args).await,
        "JSON.ARRPOP" => json::arrpop(engine, &args).await,
        "JSON.ARRTRIM" => json::arrtrim(engine, &args).await,
        "JSON.OBJKEYS" => json::objkeys(engine, &args).await,
        "JSON.OBJLEN" => json::objlen(engine, &args).await,
        "JSON.TOGGLE" => json::toggle(engine, &args).await,
        "JSON.CLEAR" => json::clear(engine, &args).await,
        "JSON.MERGE" => json::merge(engine, &args).await,
        "JSON.RESP" => json::resp(engine, &args).await,
        "JSON.DEBUG" => json::debug(engine, &args).await,
        // --- protobuf registry + typed values (PROTO.*, design/17) ---
        "PROTO.SCHEMA" => proto::schema(engine, &args).await,
        "PROTO.BIND" => proto::bind(engine, &args).await,
        "PROTO.UNBIND" => proto::unbind(engine, &args).await,
        "PROTO.BINDINGS" => proto::bindings_cmd(engine, &args).await,
        "PROTO.SET" => proto::set(engine, &args).await,
        "PROTO.GET" => proto::get(engine, &args).await,
        "PROTO.INFO" => proto::info(engine, &args).await,
        "PROTO.GETJSON" => proto::getjson(engine, &args).await,
        "PROTO.SETJSON" => proto::setjson(engine, &args).await,
        "PROTO.GETFIELD" => proto::getfield(engine, &args).await,
        "PROTO.SETFIELD" => proto::setfield(engine, &args).await,
        "PROTO.CLEARFIELD" => proto::clearfield(engine, &args).await,
        "PROTO.HSET" => proto::hset(engine, &args).await,
        "PROTO.SADD" => proto::sadd(engine, &args).await,
        "PROTO.HGETJSON" => proto::hgetjson(engine, &args).await,
        "PROTO.HGETFIELD" => proto::hgetfield(engine, &args).await,

        // --- generic / keyspace ---
        "DEL" | "UNLINK" => generic::del(engine, &args).await,
        "EXISTS" => generic::exists(engine, &args).await,
        "TYPE" => generic::type_cmd(engine, &args).await,
        "TTL" if args.len() == 3 => generic::member_ttl(engine, &args, false).await,
        "TTL" => generic::ttl(engine, &args, false).await,
        "PTTL" if args.len() == 3 => generic::member_ttl(engine, &args, true).await,
        "PTTL" => generic::ttl(engine, &args, true).await,
        "EXPIREMEMBER" => generic::expiremember(engine, &args, 1000, false).await,
        "EXPIREMEMBERAT" => generic::expiremember(engine, &args, 1000, true).await,
        "PEXPIREMEMBERAT" => generic::expiremember(engine, &args, 1, true).await,
        "EXPIRE" => generic::expire(engine, &args, 1000, false).await,
        "PEXPIRE" => generic::expire(engine, &args, 1, false).await,
        "EXPIREAT" => generic::expire(engine, &args, 1000, true).await,
        "PEXPIREAT" => generic::expire(engine, &args, 1, true).await,
        "EXPIRETIME" => generic::expiretime(engine, &args, false).await,
        "PEXPIRETIME" => generic::expiretime(engine, &args, true).await,
        "PERSIST" => generic::persist(engine, &args).await,
        "KEYS" => generic::keys(engine, &args).await,
        "SCAN" => generic::scan(engine, &args).await,
        "RANDOMKEY" => generic::randomkey(engine).await,
        "RENAME" => generic::rename(engine, &args, false).await,
        "RENAMENX" => generic::rename(engine, &args, true).await,
        "COPY" => generic::copy(engine, &args).await,
        "OBJECT" => generic::object(engine, &args).await,
        "TOUCH" => generic::exists(engine, &args).await,

        // --- strings ---
        "GET" => string::get(engine, &args).await,
        "SET" => string::set(engine, &args).await,
        "SETNX" => string::setnx(engine, &args).await,
        "SETEX" => string::setex(engine, &args, 1000).await,
        "PSETEX" => string::setex(engine, &args, 1).await,
        "GETSET" => string::getset(engine, &args).await,
        "GETDEL" => string::getdel(engine, &args).await,
        "GETEX" => string::getex(engine, &args).await,
        "APPEND" => string::append(engine, &args).await,
        "STRLEN" => string::strlen(engine, &args).await,
        "INCR" => string::incrby_cmd(engine, &args, 1, false).await,
        "DECR" => string::incrby_cmd(engine, &args, -1, false).await,
        "INCRBY" => string::incrby_cmd(engine, &args, 1, true).await,
        "DECRBY" => string::incrby_cmd(engine, &args, -1, true).await,
        "INCRBYFLOAT" => string::incrbyfloat(engine, &args).await,
        "MGET" => string::mget(engine, &args).await,
        "MSET" => string::mset(engine, &args).await,
        "MSETNX" => string::msetnx(engine, &args).await,
        "SETRANGE" => string::setrange(engine, &args).await,
        "GETRANGE" | "SUBSTR" => string::getrange(engine, &args).await,

        // --- hashes ---
        "HSET" | "HMSET" => hash::hset(engine, &args, name == "HMSET").await,
        "HSETNX" => hash::hsetnx(engine, &args).await,
        "HGET" => hash::hget(engine, &args).await,
        "HMGET" => hash::hmget(engine, &args).await,
        "HGETALL" => hash::hgetall(engine, &args).await,
        "HDEL" => hash::hdel(engine, &args).await,
        "HGETDEL" => hash::hgetdel(engine, &args).await,
        "HEXPIRE" => hash::hexpire(engine, &args, 1000, false).await,
        "HPEXPIRE" => hash::hexpire(engine, &args, 1, false).await,
        "HEXPIREAT" => hash::hexpire(engine, &args, 1000, true).await,
        "HPEXPIREAT" => hash::hexpire(engine, &args, 1, true).await,
        "HTTL" => hash::httl(engine, &args, false, false).await,
        "HPTTL" => hash::httl(engine, &args, true, false).await,
        "HEXPIRETIME" => hash::httl(engine, &args, false, true).await,
        "HPEXPIRETIME" => hash::httl(engine, &args, true, true).await,
        "HPERSIST" => hash::hpersist(engine, &args).await,
        "HGETEX" => hash::hgetex(engine, &args).await,
        "HSETEX" => hash::hsetex(engine, &args).await,
        "HEXISTS" => hash::hexists(engine, &args).await,
        "HLEN" => hash::hlen(engine, &args).await,
        "HKEYS" => hash::hkeys(engine, &args, true).await,
        "HVALS" => hash::hkeys(engine, &args, false).await,
        "HSTRLEN" => hash::hstrlen(engine, &args).await,
        "HINCRBY" => hash::hincrby(engine, &args).await,
        "HINCRBYFLOAT" => hash::hincrbyfloat(engine, &args).await,
        "HRANDFIELD" => hash::hrandfield(engine, &args).await,
        "HSCAN" => hash::hscan(engine, &args).await,

        // --- sets ---
        "SADD" => set::sadd(engine, &args).await,
        "SREM" => set::srem(engine, &args).await,
        "SCARD" => set::scard(engine, &args).await,
        "SISMEMBER" => set::sismember(engine, &args).await,
        "SMISMEMBER" => set::smismember(engine, &args).await,
        "SMEMBERS" => set::smembers(engine, &args).await,
        "SPOP" => set::spop(engine, &args).await,
        "SRANDMEMBER" => set::srandmember(engine, &args).await,
        "SSCAN" => set::sscan(engine, &args).await,
        "SMOVE" => set::smove(engine, &args).await,
        "SUNION" => set::setop(engine, &args, set::SetOp::Union, false).await,
        "SINTER" => set::setop(engine, &args, set::SetOp::Inter, false).await,
        "SDIFF" => set::setop(engine, &args, set::SetOp::Diff, false).await,
        "SUNIONSTORE" => set::setop(engine, &args, set::SetOp::Union, true).await,
        "SINTERSTORE" => set::setop(engine, &args, set::SetOp::Inter, true).await,
        "SDIFFSTORE" => set::setop(engine, &args, set::SetOp::Diff, true).await,
        "SINTERCARD" => set::sintercard(engine, &args).await,

        // --- sorted sets ---
        "ZADD" => zset::zadd(engine, &args).await,
        "ZSCORE" => zset::zscore(engine, &args).await,
        "ZMSCORE" => zset::zmscore(engine, &args).await,
        "ZCARD" => zset::zcard(engine, &args).await,
        "ZINCRBY" => zset::zincrby(engine, &args).await,
        "ZREM" => zset::zrem(engine, &args).await,
        "ZRANGE" => zset::zrange(engine, &args).await,
        "ZRANGEBYSCORE" => zset::zrangebyscore(engine, &args, false).await,
        "ZREVRANGEBYSCORE" => zset::zrangebyscore(engine, &args, true).await,
        "ZREVRANGE" => zset::zrevrange(engine, &args).await,
        "ZRANK" => zset::zrank(engine, &args, false).await,
        "ZREVRANK" => zset::zrank(engine, &args, true).await,
        "ZCOUNT" => zset::zcount(engine, &args).await,
        "ZLEXCOUNT" => zset::zlexcount(engine, &args).await,
        "ZPOPMIN" => zset::zpop(engine, &args, false).await,
        "ZPOPMAX" => zset::zpop(engine, &args, true).await,
        "BZPOPMIN" => zset::bzpop(engine, &args, false).await,
        "BZPOPMAX" => zset::bzpop(engine, &args, true).await,
        "ZMPOP" => zset::zmpop(engine, &args).await,
        "BZMPOP" => zset::bzmpop(engine, &args).await,
        "ZRANDMEMBER" => zset::zrandmember(engine, &args).await,
        "ZRANGESTORE" => zset::zrangestore(engine, &args).await,
        "ZRANGEBYLEX" => zset::zrangebylex(engine, &args, false).await,
        "ZREVRANGEBYLEX" => zset::zrangebylex(engine, &args, true).await,
        "ZREMRANGEBYSCORE" => zset::zremrangebyscore(engine, &args).await,
        "ZREMRANGEBYRANK" => zset::zremrangebyrank(engine, &args).await,
        "ZREMRANGEBYLEX" => zset::zremrangebylex(engine, &args).await,
        "ZUNION" => zset::zsetop(engine, &args, zset::ZSetOp::Union, false).await,
        "ZINTER" => zset::zsetop(engine, &args, zset::ZSetOp::Inter, false).await,
        "ZDIFF" => zset::zsetop(engine, &args, zset::ZSetOp::Diff, false).await,
        "ZUNIONSTORE" => zset::zsetop(engine, &args, zset::ZSetOp::Union, true).await,
        "ZINTERSTORE" => zset::zsetop(engine, &args, zset::ZSetOp::Inter, true).await,
        "ZDIFFSTORE" => zset::zsetop(engine, &args, zset::ZSetOp::Diff, true).await,
        "ZINTERCARD" => zset::zintercard(engine, &args).await,
        "ZSCAN" => zset::zscan(engine, &args).await,

        // --- lists ---
        "LPUSH" => list::push(engine, &args, true, false).await,
        "RPUSH" => list::push(engine, &args, false, false).await,
        "LPUSHX" => list::push(engine, &args, true, true).await,
        "RPUSHX" => list::push(engine, &args, false, true).await,
        "LPOP" => list::pop(engine, &args, true).await,
        "RPOP" => list::pop(engine, &args, false).await,
        "LLEN" => list::llen(engine, &args).await,
        "LRANGE" => list::lrange(engine, &args).await,
        "LINDEX" => list::lindex(engine, &args).await,
        "LSET" => list::lset(engine, &args).await,
        "LREM" => list::lrem(engine, &args).await,
        "LTRIM" => list::ltrim(engine, &args).await,
        "LINSERT" => list::linsert(engine, &args).await,
        "LPOS" => list::lpos(engine, &args).await,
        "LMOVE" => list::lmove(engine, &args).await,
        "RPOPLPUSH" => list::rpoplpush(engine, &args).await,
        "LMPOP" => list::lmpop(engine, &args).await,
        "BLPOP" => list::bpop(engine, &args, true).await,
        "BRPOP" => list::bpop(engine, &args, false).await,
        "BLMOVE" => list::blmove(engine, &args).await,
        "BRPOPLPUSH" => list::brpoplpush(engine, &args).await,
        "BLMPOP" => list::blmpop(engine, &args).await,

        // --- streams ---
        "XADD" => stream::xadd(engine, &args).await,
        "XLEN" => stream::xlen(engine, &args).await,
        "XRANGE" => stream::xrange(engine, &args, false).await,
        "XREVRANGE" => stream::xrange(engine, &args, true).await,
        "XREAD" => stream::xread(engine, &args).await,
        "XDEL" => stream::xdel(engine, &args).await,
        "XTRIM" => stream::xtrim(engine, &args).await,
        "XSETID" => stream::xsetid(engine, &args).await,
        "XINFO" => stream::xinfo(engine, &args).await,

        // --- Lua scripting (design/11) ---
        "EVAL" => script::eval(engine, &args).await,
        "EVALSHA" => script::evalsha(engine, &args).await,
        "SCRIPT" => script::script_cmd(engine, &args).await,

        // --- HyperLogLog ---
        "PFADD" => hll::pfadd(engine, &args).await,
        "PFCOUNT" => hll::pfcount(engine, &args).await,
        "PFMERGE" => hll::pfmerge(engine, &args).await,

        // --- pub/sub ---
        "SUBSCRIBE" => pubsub::subscribe(engine, sess, &args, out),
        "UNSUBSCRIBE" => pubsub::unsubscribe(engine, sess, &args, out),
        "PSUBSCRIBE" => pubsub::psubscribe(engine, sess, &args, out),
        "PUNSUBSCRIBE" => pubsub::punsubscribe(engine, sess, &args, out),
        "PUBLISH" => pubsub::publish(engine, &args),
        "PUBSUB" => pubsub::pubsub_cmd(engine, &args),

        _ => Reply::err(format!(
            "ERR unknown command '{}'",
            String::from_utf8_lossy(&args[0])
        )),
    }
}

// ---------------------------------------------------------------------------
// shared argument helpers
// ---------------------------------------------------------------------------

pub fn parse_i64(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

pub fn parse_u64(b: &[u8]) -> Option<u64> {
    std::str::from_utf8(b).ok()?.parse().ok()
}

pub fn parse_f64(b: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?.trim();
    match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
        "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
        _ => {
            let f: f64 = s.parse().ok()?;
            if f.is_nan() {
                None
            } else {
                Some(f)
            }
        }
    }
}

pub fn eq_ignore_case(b: &[u8], s: &str) -> bool {
    b.eq_ignore_ascii_case(s.as_bytes())
}

/// Format a float the way Redis does (no trailing .0 on integral values).
pub fn fmt_f64(f: f64) -> String {
    if f == f.trunc() && f.abs() < 1e17 && f.is_finite() {
        format!("{}", f as i64)
    } else if f == f64::INFINITY {
        "inf".into()
    } else if f == f64::NEG_INFINITY {
        "-inf".into()
    } else {
        format!("{f}")
    }
}

/// Normalize a possibly-negative index against a length, Redis style.
pub fn norm_index(idx: i64, len: usize) -> i64 {
    if idx < 0 {
        idx + len as i64
    } else {
        idx
    }
}
