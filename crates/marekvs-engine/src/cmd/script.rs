//! Lua scripting: EVAL / EVALSHA / SCRIPT (design/11).
//!
//! ## Execution model — the atomic same-pid path
//! All declared KEYS must share one partition (use hash tags `{...}` to
//! co-locate). The whole script executes as ONE shard job, so every
//! `redis.call` runs serialized against all other operations on that
//! partition — Redis-grade atomicity, node-local (design/11 caveat 1;
//! cluster-wide script atomicity is impossible in an AP system).
//!
//! `redis.call` drives the ORDINARY async command handlers through a
//! poll-once executor: thanks to the inline shard fast-path in
//! `Store::run`, every same-shard storage await resolves in a single poll.
//! Anything that would genuinely suspend — a key on another shard, a
//! blocking command, a remote fetch — returns `Poll::Pending` and becomes
//! a clean script error instead of a deadlock. Commands that spawn tasks
//! (MSET/MGET) are rejected up front.
//!
//! ## Replication
//! Effects-only, for free: script writes are ordinary records flowing
//! through the commit hook. The script itself never replicates for
//! re-execution, so `math.random`/`TIME` are permitted (design/11
//! caveat 2). Loaded scripts ALSO persist as hidden system records
//! (`\x00script:<sha>`) that replicate like data, so EVALSHA cache misses
//! on other nodes usually self-heal; the NOSCRIPT error remains the
//! correctness contract (clients retry with EVAL).
//!
//! ## Budgets
//! Instruction-count debug hook enforcing a wall deadline (default 20 ms,
//! `MAREKVS_SCRIPT_TIME_LIMIT_MS`; live-settable via `CONFIG SET
//! lua-time-limit`) and a 16 MiB Lua allocator limit. Writes made before an
//! abort stick (Redis semantics).

use std::cell::RefCell;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cmd::command_docs;
use crate::reply::Reply;
use crate::store::{read_lww, write_merged, ShardCtx};
use crate::{Engine, Session};
use marekvs_core::envelope::RecordType;
use marekvs_core::ikey;
use mlua::{Lua, LuaOptions, LuaSerdeExt, MultiValue, StdLib, Value as LuaValue, VmState};

/// Hidden system-key prefix for replicated script sources. `\x00` cannot
/// collide with printable user keys and is filtered from SCAN/KEYS/DBSIZE.
pub const SCRIPT_SYS_PREFIX: &[u8] = b"\x00script:";

fn time_limit(engine: &Engine) -> Duration {
    Duration::from_millis(
        engine
            .script_time_limit_ms
            .load(std::sync::atomic::Ordering::Relaxed),
    )
}

pub fn sha1_hex(data: &[u8]) -> String {
    let mut h = sha1_smol::Sha1::new();
    h.update(data);
    h.digest().to_string()
}

/// Commands callable from scripts: parallel-safe data commands MINUS those
/// that spawn tokio tasks (no runtime on shard threads) or block.
fn script_safe(name: &str) -> bool {
    if matches!(
        name,
        "MSET" | "MGET" | "SUBSCRIBE" | "EVAL" | "EVALSHA" | "SCRIPT"
    ) {
        return false;
    }
    // PROTO.* is excluded wholesale in v1: typed handlers consult the
    // hidden registry (a different partition) and may spawn_blocking —
    // both suspend inside the poll-once script driver.
    if name.starts_with("PROTO.") {
        return false;
    }
    Engine::parallel_safe(name) || matches!(name, "PING" | "ECHO" | "TIME")
}

/// Poll a future exactly once. `Ready` = completed synchronously (the
/// inline shard path). `Pending` = it tried to actually suspend.
fn poll_once<F: std::future::Future>(fut: F) -> Option<F::Output> {
    let mut fut = Box::pin(fut);
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    match fut.as_mut().poll(&mut cx) {
        std::task::Poll::Ready(v) => Some(v),
        std::task::Poll::Pending => None,
    }
}

// ---------------------------------------------------------------------------
// Lua <-> RESP conversion (the Redis conversion table)
// ---------------------------------------------------------------------------

fn reply_to_lua(lua: &Lua, reply: Reply) -> mlua::Result<LuaValue> {
    Ok(match reply {
        Reply::Null | Reply::NullArray => LuaValue::Boolean(false),
        Reply::Int(i) => LuaValue::Integer(i),
        Reply::Bulk(b) => LuaValue::String(lua.create_string(&b)?),
        Reply::Simple(s) => {
            let t = lua.create_table()?;
            t.set("ok", s)?;
            LuaValue::Table(t)
        }
        Reply::SimpleOwned(s) => {
            let t = lua.create_table()?;
            t.set("ok", s)?;
            LuaValue::Table(t)
        }
        Reply::Err(e) => {
            let t = lua.create_table()?;
            t.set("err", e)?;
            LuaValue::Table(t)
        }
        Reply::Array(items) | Reply::Set(items) => {
            let t = lua.create_table()?;
            for (i, item) in items.into_iter().enumerate() {
                t.set(i + 1, reply_to_lua(lua, item)?)?;
            }
            LuaValue::Table(t)
        }
        Reply::Map(pairs) => {
            // RESP2 conversion: flatten to [k1, v1, k2, v2, ...].
            let t = lua.create_table()?;
            let mut idx = 1;
            for (k, v) in pairs {
                t.set(idx, reply_to_lua(lua, k)?)?;
                t.set(idx + 1, reply_to_lua(lua, v)?)?;
                idx += 2;
            }
            LuaValue::Table(t)
        }
        Reply::Double(d) => LuaValue::String(lua.create_string(crate::cmd::fmt_f64(d))?),
        Reply::Bool(b) => LuaValue::Integer(b as i64),
        Reply::Verbatim(s) => LuaValue::String(lua.create_string(&s)?),
        Reply::None => LuaValue::Boolean(false),
    })
}

fn lua_to_reply(v: &LuaValue) -> Reply {
    match v {
        LuaValue::Nil => Reply::Null,
        LuaValue::Boolean(true) => Reply::Int(1),
        LuaValue::Boolean(false) => Reply::Null,
        LuaValue::Integer(i) => Reply::Int(*i),
        LuaValue::Number(n) => Reply::Int(*n as i64), // Redis truncates
        LuaValue::String(s) => Reply::Bulk(s.as_bytes().to_vec()),
        LuaValue::Table(t) => {
            if let Ok(err) = t.get::<String>("err") {
                if !err.is_empty() {
                    return Reply::Err(err);
                }
            }
            if let Ok(ok) = t.get::<String>("ok") {
                if !ok.is_empty() {
                    return Reply::SimpleOwned(ok);
                }
            }
            // Array part until the first nil (Redis rule).
            let mut items = Vec::new();
            for i in 1.. {
                match t.get::<LuaValue>(i) {
                    Ok(LuaValue::Nil) | Err(_) => break,
                    Ok(item) => items.push(lua_to_reply(&item)),
                }
            }
            Reply::Array(items)
        }
        _ => Reply::Null,
    }
}

// ---------------------------------------------------------------------------
// sandboxed Lua instance (thread-local per shard, reused across calls)
// ---------------------------------------------------------------------------

thread_local! {
    static SHARD_LUA: RefCell<Option<Lua>> = const { RefCell::new(None) };
}

fn build_lua() -> mlua::Result<Lua> {
    // Redis-style sandbox: no os/io/package/debug. `load` stays (scripts
    // may not call it into files — no io — and Redis exposes it too).
    let lua = Lua::new_with(
        StdLib::MATH | StdLib::STRING | StdLib::TABLE,
        LuaOptions::default(),
    )?;
    lua.set_memory_limit(16 * 1024 * 1024)?;

    // bit library shim (LuaBitOp semantics on 32-bit values) — Redis
    // scripts written for Lua 5.1 call these.
    let bit = lua.create_table()?;
    macro_rules! bitfn {
        ($name:expr, $f:expr) => {
            bit.set($name, lua.create_function($f)?)?;
        };
    }
    bitfn!("band", |_, (a, b): (i64, i64)| Ok(
        ((a as u32) & (b as u32)) as i64
    ));
    bitfn!("bor", |_, (a, b): (i64, i64)| Ok(
        ((a as u32) | (b as u32)) as i64
    ));
    bitfn!("bxor", |_, (a, b): (i64, i64)| Ok(
        ((a as u32) ^ (b as u32)) as i64
    ));
    bitfn!("bnot", |_, a: i64| Ok(!(a as u32) as i64));
    bitfn!("lshift", |_, (a, b): (i64, i64)| Ok(
        ((a as u32) << (b as u32 & 31)) as i64
    ));
    bitfn!("rshift", |_, (a, b): (i64, i64)| Ok(
        ((a as u32) >> (b as u32 & 31)) as i64
    ));
    bitfn!("arshift", |_, (a, b): (i64, i64)| Ok(
        ((a as i32) >> (b as u32 & 31)) as i64
    ));
    bitfn!("tobit", |_, a: i64| Ok(a as u32 as i64));
    lua.globals().set("bit", bit)?;

    // cjson via serde_json.
    let cjson = lua.create_table()?;
    cjson.set(
        "encode",
        lua.create_function(|lua, v: LuaValue| {
            let json: serde_json::Value = lua.from_value(v)?;
            serde_json::to_string(&json).map_err(mlua::Error::external)
        })?,
    )?;
    cjson.set(
        "decode",
        lua.create_function(|lua, s: mlua::String| {
            let json: serde_json::Value =
                serde_json::from_slice(&s.as_bytes()).map_err(mlua::Error::external)?;
            lua.to_value(&json)
        })?,
    )?;
    lua.globals().set("cjson", cjson)?;
    Ok(lua)
}

// ---------------------------------------------------------------------------
// script execution
// ---------------------------------------------------------------------------

struct ScriptRun {
    source: String,
    keys: Vec<Vec<u8>>,
    argv: Vec<Vec<u8>>,
}

/// Execute a script on the current shard thread. Called from inside a
/// `Store::run` job, so `redis.call` storage awaits resolve inline.
fn run_on_shard(engine: &Arc<Engine>, _ctx: &ShardCtx, run: ScriptRun) -> Reply {
    SHARD_LUA.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            match build_lua() {
                Ok(l) => *slot = Some(l),
                Err(e) => return Reply::err(format!("ERR Lua init failed: {e}")),
            }
        }
        let lua = slot.as_ref().unwrap();
        execute(engine, lua, run)
    })
}

fn execute(engine: &Arc<Engine>, lua: &Lua, run: ScriptRun) -> Reply {
    // KEYS / ARGV globals.
    let set_arrays = || -> mlua::Result<()> {
        let keys_t = lua.create_table()?;
        for (i, k) in run.keys.iter().enumerate() {
            keys_t.set(i + 1, lua.create_string(k)?)?;
        }
        let argv_t = lua.create_table()?;
        for (i, a) in run.argv.iter().enumerate() {
            argv_t.set(i + 1, lua.create_string(a)?)?;
        }
        lua.globals().set("KEYS", keys_t)?;
        lua.globals().set("ARGV", argv_t)?;
        Ok(())
    };
    if let Err(e) = set_arrays() {
        return Reply::err(format!("ERR script setup: {e}"));
    }

    // redis.call / redis.pcall bridge, capturing the declared key set.
    let declared: Arc<Vec<Vec<u8>>> = Arc::new(run.keys.clone());
    let make_call = |raise: bool| {
        let engine = engine.clone();
        let declared = declared.clone();
        move |lua: &Lua, args: MultiValue| -> mlua::Result<LuaValue> {
            let mut argv: Vec<Vec<u8>> = Vec::with_capacity(args.len());
            for a in args {
                match a {
                    LuaValue::String(s) => argv.push(s.as_bytes().to_vec()),
                    LuaValue::Integer(i) => argv.push(i.to_string().into_bytes()),
                    LuaValue::Number(n) => {
                        argv.push(crate::cmd::fmt_f64(n).into_bytes());
                    }
                    _ => {
                        return Err(mlua::Error::external(
                            "redis.call arguments must be strings or numbers",
                        ))
                    }
                }
            }
            let reply = bridge_call(&engine, &declared, argv);
            match reply {
                Reply::Err(e) if raise => Err(mlua::Error::external(e)),
                other => reply_to_lua(lua, other),
            }
        }
    };
    let setup_redis = || -> mlua::Result<()> {
        let redis = lua.create_table()?;
        redis.set("call", lua.create_function(make_call(true))?)?;
        redis.set("pcall", lua.create_function(make_call(false))?)?;
        redis.set(
            "error_reply",
            lua.create_function(|lua, msg: String| {
                let t = lua.create_table()?;
                let msg = if msg.contains(' ')
                    && msg
                        .split(' ')
                        .next()
                        .unwrap()
                        .chars()
                        .all(|c| c.is_ascii_uppercase())
                {
                    msg
                } else {
                    format!("ERR {msg}")
                };
                t.set("err", msg)?;
                Ok(t)
            })?,
        )?;
        redis.set(
            "status_reply",
            lua.create_function(|lua, msg: String| {
                let t = lua.create_table()?;
                t.set("ok", msg)?;
                Ok(t)
            })?,
        )?;
        redis.set(
            "sha1hex",
            lua.create_function(|_, s: mlua::String| Ok(sha1_hex(&s.as_bytes())))?,
        )?;
        // Effects replication is inherent; scripts calling this expect true.
        redis.set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
        redis.set("LOG_DEBUG", 0)?;
        redis.set("LOG_VERBOSE", 1)?;
        redis.set("LOG_NOTICE", 2)?;
        redis.set("LOG_WARNING", 3)?;
        redis.set(
            "log",
            lua.create_function(|_, (_level, msg): (i64, String)| {
                tracing::info!(target: "marekvs::script", "{msg}");
                Ok(())
            })?,
        )?;
        lua.globals().set("redis", redis)?;
        Ok(())
    };
    if let Err(e) = setup_redis() {
        return Reply::err(format!("ERR script setup: {e}"));
    }

    // Instruction-budget hook: abort when the wall deadline passes.
    let deadline = Instant::now() + time_limit(engine);
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(10_000),
        move |_lua, _debug| {
            if Instant::now() >= deadline {
                Err(mlua::Error::external(
                    "script exceeded time limit (writes already made are kept)",
                ))
            } else {
                Ok(VmState::Continue)
            }
        },
    );

    let result = lua
        .load(run.source.as_str())
        .set_name("@user_script")
        .eval::<LuaValue>();
    lua.remove_hook();
    // Drop globals we set so state can't leak between scripts.
    let _ = lua.globals().set("KEYS", LuaValue::Nil);
    let _ = lua.globals().set("ARGV", LuaValue::Nil);

    match result {
        Ok(v) => lua_to_reply(&v),
        Err(e) => {
            let msg = e.to_string();
            let first = msg.lines().next().unwrap_or("script error");
            Reply::err(format!("ERR Error running script: {first}"))
        }
    }
}

/// One `redis.call` from inside a script: validate, key-check, poll-once.
fn bridge_call(engine: &Arc<Engine>, declared: &[Vec<u8>], argv: Vec<Vec<u8>>) -> Reply {
    let Some(name) = argv.first() else {
        return Reply::err("ERR wrong number of arguments");
    };
    let name = String::from_utf8_lossy(name).to_uppercase();
    if !script_safe(&name) {
        return Reply::err(format!("ERR command '{name}' is not allowed from scripts"));
    }
    // Every key the command touches must be DECLARED in KEYS (Redis
    // Cluster discipline; also what guarantees same-shard execution).
    if let Some(doc) = command_docs::find(&name) {
        for key in command_docs::extract_keys(doc, &argv) {
            if !declared.iter().any(|k| k == key) {
                return Reply::err(format!(
                    "ERR script accessed undeclared key '{}' — declare it in KEYS",
                    String::from_utf8_lossy(key)
                ));
            }
        }
    }
    let fut = async {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let mut sess = Session::new(u64::MAX, tx);
        sess.authenticated = true;
        let mut out = marekvs_resp::ReplyBuf::new(false);
        let reply = crate::cmd::dispatch(engine, &mut sess, &name, argv, &mut out).await;
        (reply, out)
    };
    match poll_once(fut) {
        Some((reply, out)) if out.is_empty() => reply,
        Some(_) => Reply::err("ERR command produced out-of-band output in script"),
        None => Reply::err(
            "ERR command suspended inside script (key on another shard, remote fetch, \
             or blocking op) — co-locate keys with hash tags {...}",
        ),
    }
}

// ---------------------------------------------------------------------------
// EVAL / EVALSHA / SCRIPT command handlers
// ---------------------------------------------------------------------------

type KeysArgv = (Vec<Vec<u8>>, Vec<Vec<u8>>);

fn parse_eval(args: &[Vec<u8>]) -> Result<KeysArgv, Reply> {
    let Some(numkeys) = crate::cmd::parse_i64(&args[2]) else {
        return Err(Reply::not_int());
    };
    if numkeys < 0 {
        return Err(Reply::err("ERR Number of keys can't be negative"));
    }
    let numkeys = numkeys as usize;
    if args.len() < 3 + numkeys {
        return Err(Reply::err(
            "ERR Number of keys can't be greater than number of args",
        ));
    }
    let keys: Vec<Vec<u8>> = args[3..3 + numkeys].to_vec();
    let argv: Vec<Vec<u8>> = args[3 + numkeys..].to_vec();
    Ok((keys, argv))
}

async fn eval_source(
    engine: &Arc<Engine>,
    source: String,
    keys: Vec<Vec<u8>>,
    argv: Vec<Vec<u8>>,
) -> Reply {
    // All declared keys must share one partition (hash tags co-locate).
    let pid = match keys.first() {
        Some(k) => marekvs_core::pid_of(k),
        None => 0, // keyless script: any shard works, no key access allowed
    };
    for k in &keys[1.min(keys.len())..] {
        if marekvs_core::pid_of(k) != pid {
            return Reply::err(
                "CROSSSLOT Keys in request don't hash to the same partition — \
                 use hash tags {...} to co-locate script keys",
            );
        }
    }
    // Pre-fetch declared keys (async, before entering the shard job).
    for k in &keys {
        engine.ensure_local(k).await;
    }
    let engine2 = engine.clone();
    engine
        .store
        .run(pid, move |ctx| {
            run_on_shard(&engine2, ctx, ScriptRun { source, keys, argv })
        })
        .await
}

pub async fn eval(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("eval");
    }
    let source = String::from_utf8_lossy(&args[1]).into_owned();
    let (keys, argv) = match parse_eval(args) {
        Ok(v) => v,
        Err(r) => return r,
    };
    // EVAL also populates the cache (Redis behavior).
    let sha = sha1_hex(source.as_bytes());
    engine.scripts.write().insert(sha, source.clone());
    eval_source(engine, source, keys, argv).await
}

pub async fn evalsha(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 3 {
        return Reply::wrong_args("evalsha");
    }
    let sha = String::from_utf8_lossy(&args[1]).to_lowercase();
    let source = engine.scripts.read().get(&sha).cloned();
    let source = match source {
        Some(s) => s,
        None => {
            // Replicated system record fallback (design/11 caveat 5).
            let syskey = [SCRIPT_SYS_PREFIX, sha.as_bytes()].concat();
            engine.ensure_local(&syskey).await;
            let sk = syskey.clone();
            let found = engine
                .store
                .run_key(&syskey, move |ctx| {
                    read_lww(ctx, &ikey::string_key(&sk), 0).map(|(_, payload)| payload)
                })
                .await;
            match found {
                Some(src) => {
                    let src = String::from_utf8_lossy(&src).into_owned();
                    engine.scripts.write().insert(sha.clone(), src.clone());
                    src
                }
                None => return Reply::err("NOSCRIPT No matching script. Please use EVAL."),
            }
        }
    };
    let (keys, argv) = match parse_eval(args) {
        Ok(v) => v,
        Err(r) => return r,
    };
    eval_source(engine, source, keys, argv).await
}

pub async fn script_cmd(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("script");
    }
    let sub = String::from_utf8_lossy(&args[1]).to_uppercase();
    match sub.as_str() {
        "LOAD" => {
            let Some(src) = args.get(2) else {
                return Reply::wrong_args("script|load");
            };
            let source = String::from_utf8_lossy(src).into_owned();
            let sha = sha1_hex(source.as_bytes());
            engine.scripts.write().insert(sha.clone(), source.clone());
            // Persist as a hidden replicated record so other nodes can
            // serve EVALSHA after replication/anti-entropy delivers it.
            let syskey = [SCRIPT_SYS_PREFIX, sha.as_bytes()].concat();
            engine.ensure_local(&syskey).await;
            let sk = syskey.clone();
            engine
                .store
                .run_key(&syskey, move |ctx| {
                    let rec = crate::store::new_lww(ctx, RecordType::String, source.as_bytes(), 0);
                    write_merged(ctx, &ikey::string_key(&sk), &rec);
                })
                .await;
            Reply::Bulk(sha.into_bytes())
        }
        "EXISTS" => Reply::Array(
            args[2..]
                .iter()
                .map(|s| {
                    let sha = String::from_utf8_lossy(s).to_lowercase();
                    Reply::Int(engine.scripts.read().contains_key(&sha) as i64)
                })
                .collect(),
        ),
        "FLUSH" => {
            engine.scripts.write().clear();
            Reply::ok()
        }
        _ => Reply::err(format!("ERR Unknown SCRIPT subcommand '{sub}'")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_matches_redis() {
        // redis-cli SCRIPT LOAD "return 1" → e0e1f9fabfc9d4800c877a703b823ac0578ff8db
        assert_eq!(
            sha1_hex(b"return 1"),
            "e0e1f9fabfc9d4800c877a703b823ac0578ff8db"
        );
    }

    #[test]
    fn lua_reply_conversion_table() {
        let lua = build_lua().unwrap();
        // nil → Null, false → Null, true → 1, number truncates
        assert_eq!(lua_to_reply(&LuaValue::Nil), Reply::Null);
        assert_eq!(lua_to_reply(&LuaValue::Boolean(false)), Reply::Null);
        assert_eq!(lua_to_reply(&LuaValue::Boolean(true)), Reply::Int(1));
        assert_eq!(lua_to_reply(&LuaValue::Number(3.9)), Reply::Int(3));
        // {ok=...} / {err=...}
        let t = lua.create_table().unwrap();
        t.set("ok", "FINE").unwrap();
        assert_eq!(
            lua_to_reply(&LuaValue::Table(t)),
            Reply::SimpleOwned("FINE".into())
        );
        // array stops at first nil
        let t = lua.create_table().unwrap();
        t.set(1, 10).unwrap();
        t.set(2, 20).unwrap();
        t.set(4, 40).unwrap(); // hole → ignored from 3 on
        assert_eq!(
            lua_to_reply(&LuaValue::Table(t)),
            Reply::Array(vec![Reply::Int(10), Reply::Int(20)])
        );
    }

    #[test]
    fn sandbox_blocks_os_io() {
        let lua = build_lua().unwrap();
        let v: LuaValue = lua.load("return os").eval().unwrap();
        assert!(matches!(v, LuaValue::Nil));
        let v: LuaValue = lua.load("return io").eval().unwrap();
        assert!(matches!(v, LuaValue::Nil));
    }

    #[test]
    fn bit_and_cjson_shims() {
        let lua = build_lua().unwrap();
        let v: i64 = lua.load("return bit.band(12, 10)").eval().unwrap();
        assert_eq!(v, 8);
        let v: String = lua.load("return cjson.encode({1, 2, 3})").eval().unwrap();
        assert_eq!(v, "[1,2,3]");
        let v: i64 = lua.load("return cjson.decode('[5,6]')[2]").eval().unwrap();
        assert_eq!(v, 6);
    }
}
