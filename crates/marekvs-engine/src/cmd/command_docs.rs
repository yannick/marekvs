//! Static command catalog powering `COMMAND` / `COMMAND INFO` /
//! `COMMAND COUNT` / `COMMAND DOCS` / `COMMAND GETKEYS`.
//!
//! The table lists exactly the commands the dispatcher serves — every arm in
//! [`crate::cmd::dispatch`] plus the transaction verbs (`MULTI`/`EXEC`/
//! `DISCARD`/`WATCH`/`UNWATCH`) handled one layer up in
//! [`crate::Engine::dispatch`]. redis-cli 7+/8 fetches `COMMAND DOCS` at
//! connect and builds tab-completion + inline hints from it; the `arguments`
//! trees below are shaped to match a real redis-server so redis-cli's hint
//! walker consumes them unchanged.

use crate::reply::Reply;

/// One argument in a command's syntax spec.
///
/// Leaf args (`key`/`string`/`integer`/`pure-token`/…) render `name`, `type`,
/// `display_text` and any of `token`/`since`/`key_spec_index`/`flags`.
/// Container args (`oneof`/`block`) render `name`, `type`, `flags`,
/// `arguments` and — matching real redis — omit `display_text`.
#[derive(Debug, Clone, Copy)]
pub struct Arg {
    pub name: &'static str,
    pub typ: &'static str,
    pub token: Option<&'static str>,
    pub since: Option<&'static str>,
    pub key_spec_index: Option<i64>,
    pub optional: bool,
    pub multiple: bool,
    pub args: &'static [Arg],
}

const fn arg(name: &'static str, typ: &'static str) -> Arg {
    Arg {
        name,
        typ,
        token: None,
        since: None,
        key_spec_index: None,
        optional: false,
        multiple: false,
        args: &[],
    }
}

/// A `key`-typed leaf pointing at key spec 0 (the common single-key case).
const A_KEY: Arg = Arg {
    key_spec_index: Some(0),
    ..arg("key", "key")
};

/// One documented command.
#[derive(Debug, Clone, Copy)]
pub struct CommandDoc {
    pub name: &'static str,
    pub arity: i64,
    pub flags: &'static [&'static str],
    pub first_key: i64,
    pub last_key: i64,
    pub step: i64,
    pub summary: &'static str,
    pub since: &'static str,
    pub group: &'static str,
    pub complexity: &'static str,
    pub args: &'static [Arg],
}

#[allow(clippy::too_many_arguments)]
const fn cmd(
    name: &'static str,
    arity: i64,
    flags: &'static [&'static str],
    first_key: i64,
    last_key: i64,
    step: i64,
    summary: &'static str,
    since: &'static str,
    group: &'static str,
) -> CommandDoc {
    CommandDoc {
        name,
        arity,
        flags,
        first_key,
        last_key,
        step,
        summary,
        since,
        group,
        complexity: "O(1)",
        args: &[],
    }
}

const fn with_args(mut d: CommandDoc, args: &'static [Arg]) -> CommandDoc {
    d.args = args;
    d
}

const fn with_complexity(mut d: CommandDoc, c: &'static str) -> CommandDoc {
    d.complexity = c;
    d
}

// --- shared arg slices -----------------------------------------------------

const ARGS_KEY: &[Arg] = &[A_KEY];
const ARGS_KEYS: &[Arg] = &[Arg {
    multiple: true,
    ..A_KEY
}];

// GET / SET / HSET / LPUSH argument trees are matched field-for-field against
// a real redis-server (see the module doc); keep them in sync if edited.
const ARGS_GET: &[Arg] = &[A_KEY];

const SET_CONDITION: &[Arg] = &[
    Arg {
        token: Some("NX"),
        ..arg("nx", "pure-token")
    },
    Arg {
        token: Some("XX"),
        ..arg("xx", "pure-token")
    },
];

const SET_EXPIRATION: &[Arg] = &[
    Arg {
        token: Some("EX"),
        ..arg("seconds", "integer")
    },
    Arg {
        token: Some("PX"),
        ..arg("milliseconds", "integer")
    },
    Arg {
        token: Some("EXAT"),
        ..arg("unix-time-seconds", "unix-time")
    },
    Arg {
        token: Some("PXAT"),
        ..arg("unix-time-milliseconds", "unix-time")
    },
    Arg {
        token: Some("KEEPTTL"),
        ..arg("keepttl", "pure-token")
    },
];

const ARGS_SET: &[Arg] = &[
    A_KEY,
    arg("value", "string"),
    Arg {
        optional: true,
        args: SET_CONDITION,
        ..arg("condition", "oneof")
    },
    Arg {
        token: Some("GET"),
        since: Some("6.2.0"),
        optional: true,
        ..arg("get", "pure-token")
    },
    Arg {
        optional: true,
        args: SET_EXPIRATION,
        ..arg("expiration", "oneof")
    },
];

const HSET_DATA: &[Arg] = &[arg("field", "string"), arg("value", "string")];

const ARGS_HSET: &[Arg] = &[
    A_KEY,
    Arg {
        multiple: true,
        args: HSET_DATA,
        ..arg("data", "block")
    },
];

const ARGS_PUSH: &[Arg] = &[
    A_KEY,
    Arg {
        multiple: true,
        ..arg("element", "string")
    },
];

const ARGS_KEY_VALUE: &[Arg] = &[A_KEY, arg("value", "string")];
const ARGS_KEY_MEMBERS: &[Arg] = &[
    A_KEY,
    Arg {
        multiple: true,
        ..arg("member", "string")
    },
];
const ARGS_KEY_FIELD: &[Arg] = &[A_KEY, arg("field", "string")];

// ---------------------------------------------------------------------------
// The catalog. Order groups the way design/03-redis-api.md does.
// ---------------------------------------------------------------------------

const CF: &[&str] = &["readonly", "fast"]; // read, fast
const CW: &[&str] = &["write", "denyoom"]; // write, may allocate
const CWF: &[&str] = &["write", "denyoom", "fast"];
const CRO: &[&str] = &["readonly"];
const CWM: &[&str] = &["write", "denyoom", "movablekeys"];
const CROM: &[&str] = &["readonly", "movablekeys"];
const CBM: &[&str] = &["write", "blocking", "movablekeys"];

const COPY_OPTS: &[Arg] = &[
    Arg {
        token: Some("DB"),
        optional: true,
        ..arg("destination-db", "integer")
    },
    Arg {
        token: Some("REPLACE"),
        optional: true,
        ..arg("replace", "pure-token")
    },
];

const OBJECT_ARGS: &[Arg] = &[
    arg("subcommand", "string"),
    Arg {
        optional: true,
        ..A_KEY
    },
];

const HASH_FIELDS: &[Arg] = &[
    Arg {
        token: Some("FIELDS"),
        ..arg("fields", "pure-token")
    },
    arg("numfields", "integer"),
    Arg {
        multiple: true,
        ..arg("field", "string")
    },
];

const HASH_EXPIRE_COND: &[Arg] = &[
    Arg {
        token: Some("NX"),
        ..arg("nx", "pure-token")
    },
    Arg {
        token: Some("XX"),
        ..arg("xx", "pure-token")
    },
    Arg {
        token: Some("GT"),
        ..arg("gt", "pure-token")
    },
    Arg {
        token: Some("LT"),
        ..arg("lt", "pure-token")
    },
];

const HASH_EXPIRE_ARGS: &[Arg] = &[
    A_KEY,
    arg("time", "integer"),
    Arg {
        optional: true,
        args: HASH_EXPIRE_COND,
        ..arg("condition", "oneof")
    },
    Arg {
        args: HASH_FIELDS,
        ..arg("fields", "block")
    },
];

const HASH_TTL_ARGS: &[Arg] = &[
    A_KEY,
    Arg {
        args: HASH_FIELDS,
        ..arg("fields", "block")
    },
];

const HGETEX_EXPIRATION: &[Arg] = &[
    Arg {
        token: Some("EX"),
        ..arg("seconds", "integer")
    },
    Arg {
        token: Some("PX"),
        ..arg("milliseconds", "integer")
    },
    Arg {
        token: Some("EXAT"),
        ..arg("unix-time-seconds", "unix-time")
    },
    Arg {
        token: Some("PXAT"),
        ..arg("unix-time-milliseconds", "unix-time")
    },
    Arg {
        token: Some("PERSIST"),
        ..arg("persist", "pure-token")
    },
];

const HGETEX_ARGS: &[Arg] = &[
    A_KEY,
    Arg {
        optional: true,
        args: HGETEX_EXPIRATION,
        ..arg("expiration", "oneof")
    },
    Arg {
        args: HASH_FIELDS,
        ..arg("fields", "block")
    },
];

const HSETEX_COND: &[Arg] = &[
    Arg {
        token: Some("FNX"),
        ..arg("fnx", "pure-token")
    },
    Arg {
        token: Some("FXX"),
        ..arg("fxx", "pure-token")
    },
];

const HSETEX_EXPIRATION: &[Arg] = &[
    Arg {
        token: Some("EX"),
        ..arg("seconds", "integer")
    },
    Arg {
        token: Some("PX"),
        ..arg("milliseconds", "integer")
    },
    Arg {
        token: Some("EXAT"),
        ..arg("unix-time-seconds", "unix-time")
    },
    Arg {
        token: Some("PXAT"),
        ..arg("unix-time-milliseconds", "unix-time")
    },
    Arg {
        token: Some("KEEPTTL"),
        ..arg("keepttl", "pure-token")
    },
];

const HSETEX_FVS: &[Arg] = &[
    Arg {
        token: Some("FVS"),
        ..arg("fvs", "pure-token")
    },
    arg("num-field-value-pairs", "integer"),
    Arg {
        multiple: true,
        args: &[arg("field", "string"), arg("value", "string")],
        ..arg("field-value", "block")
    },
];

const HSETEX_ARGS: &[Arg] = &[
    A_KEY,
    Arg {
        optional: true,
        args: HSETEX_COND,
        ..arg("condition", "oneof")
    },
    Arg {
        optional: true,
        args: HSETEX_EXPIRATION,
        ..arg("expiration", "oneof")
    },
    Arg {
        args: HSETEX_FVS,
        ..arg("field-values", "block")
    },
];

const ZRANGE_OPTS: &[Arg] = &[
    Arg {
        token: Some("BYSCORE"),
        optional: true,
        ..arg("byscore", "pure-token")
    },
    Arg {
        token: Some("BYLEX"),
        optional: true,
        ..arg("bylex", "pure-token")
    },
    Arg {
        token: Some("REV"),
        optional: true,
        ..arg("rev", "pure-token")
    },
    Arg {
        token: Some("LIMIT"),
        optional: true,
        args: &[arg("offset", "integer"), arg("count", "integer")],
        ..arg("limit", "block")
    },
    Arg {
        token: Some("WITHSCORES"),
        optional: true,
        ..arg("withscores", "pure-token")
    },
];

const ZRANGE_ARGS: &[Arg] = &[
    A_KEY,
    arg("start", "string"),
    arg("stop", "string"),
    Arg {
        optional: true,
        multiple: true,
        args: ZRANGE_OPTS,
        ..arg("options", "oneof")
    },
];

const ZRANGESTORE_ARGS: &[Arg] = &[
    Arg {
        key_spec_index: Some(0),
        ..arg("dst", "key")
    },
    Arg {
        key_spec_index: Some(1),
        ..arg("src", "key")
    },
    arg("start", "string"),
    arg("stop", "string"),
    Arg {
        optional: true,
        multiple: true,
        args: ZRANGE_OPTS,
        ..arg("options", "oneof")
    },
];

const ZLEX_RANGE_ARGS: &[Arg] = &[
    A_KEY,
    arg("min", "string"),
    arg("max", "string"),
    Arg {
        token: Some("LIMIT"),
        optional: true,
        args: &[arg("offset", "integer"), arg("count", "integer")],
        ..arg("limit", "block")
    },
];

const ZMPOP_TAIL: &[Arg] = &[
    Arg {
        multiple: true,
        ..arg("key", "key")
    },
    arg("where", "oneof"),
    Arg {
        token: Some("COUNT"),
        optional: true,
        ..arg("count", "integer")
    },
];

const ZMPOP_ARGS: &[Arg] = &[
    arg("numkeys", "integer"),
    Arg {
        args: ZMPOP_TAIL,
        ..arg("data", "block")
    },
];

const BZMPOP_ARGS: &[Arg] = &[
    arg("timeout", "double"),
    arg("numkeys", "integer"),
    Arg {
        args: ZMPOP_TAIL,
        ..arg("data", "block")
    },
];

const ZSETOP_ARGS: &[Arg] = &[
    arg("numkeys", "integer"),
    Arg {
        multiple: true,
        ..arg("key", "key")
    },
    Arg {
        token: Some("WEIGHTS"),
        optional: true,
        multiple: true,
        ..arg("weight", "double")
    },
    Arg {
        token: Some("AGGREGATE"),
        optional: true,
        ..arg("aggregate", "oneof")
    },
    Arg {
        token: Some("WITHSCORES"),
        optional: true,
        ..arg("withscores", "pure-token")
    },
];

const ZSETSTORE_ARGS: &[Arg] = &[
    Arg {
        key_spec_index: Some(0),
        ..arg("destination", "key")
    },
    arg("numkeys", "integer"),
    Arg {
        multiple: true,
        key_spec_index: Some(1),
        ..arg("key", "key")
    },
    Arg {
        token: Some("WEIGHTS"),
        optional: true,
        multiple: true,
        ..arg("weight", "double")
    },
    Arg {
        token: Some("AGGREGATE"),
        optional: true,
        ..arg("aggregate", "oneof")
    },
];

const ZINTERCARD_ARGS: &[Arg] = &[
    arg("numkeys", "integer"),
    Arg {
        multiple: true,
        ..arg("key", "key")
    },
    Arg {
        token: Some("LIMIT"),
        optional: true,
        ..arg("limit", "integer")
    },
];

const LMPOP_TAIL: &[Arg] = &[
    Arg {
        multiple: true,
        ..arg("key", "key")
    },
    arg("where", "oneof"),
    Arg {
        token: Some("COUNT"),
        optional: true,
        ..arg("count", "integer")
    },
];

const LMPOP_ARGS: &[Arg] = &[
    arg("numkeys", "integer"),
    Arg {
        args: LMPOP_TAIL,
        ..arg("data", "block")
    },
];

const BLMPOP_ARGS: &[Arg] = &[
    arg("timeout", "double"),
    arg("numkeys", "integer"),
    Arg {
        args: LMPOP_TAIL,
        ..arg("data", "block")
    },
];

const XSETID_ARGS: &[Arg] = &[
    A_KEY,
    arg("last-id", "string"),
    Arg {
        token: Some("ENTRIESADDED"),
        optional: true,
        ..arg("entries-added", "integer")
    },
    Arg {
        token: Some("MAXDELETEDID"),
        optional: true,
        ..arg("max-deleted-id", "string")
    },
];

const XINFO_ARGS: &[Arg] = &[
    arg("subcommand", "string"),
    Arg {
        optional: true,
        ..A_KEY
    },
];

#[rustfmt::skip]
static TABLE: &[CommandDoc] = &[
    // --- connection / server ---
    with_args(cmd("ping", -1, &["fast"], 0, 0, 0, "Returns the server's liveliness response.", "1.0.0", "connection"),
        &[Arg { optional: true, ..arg("message", "string") }]),
    with_args(cmd("echo", 2, &["fast"], 0, 0, 0, "Returns the given string.", "1.0.0", "connection"),
        &[arg("message", "string")]),
    with_args(cmd("hello", -1, &["noscript", "fast"], 0, 0, 0, "Handshakes with the server, optionally switching protocol.", "6.0.0", "connection"),
        &[Arg { optional: true, ..arg("protover", "integer") }]),
    with_args(cmd("auth", -2, &["noscript", "fast"], 0, 0, 0, "Authenticates the connection.", "1.0.0", "connection"),
        &[Arg { optional: true, ..arg("username", "string") }, arg("password", "string")]),
    cmd("quit", 1, &["noscript", "fast"], 0, 0, 0, "Closes the connection.", "1.0.0", "connection"),
    cmd("reset", 1, &["noscript", "fast"], 0, 0, 0, "Resets the connection.", "6.2.0", "connection"),
    with_args(cmd("select", 2, &["fast"], 0, 0, 0, "Changes the selected database (only DB 0 is supported).", "1.0.0", "connection"),
        &[arg("index", "integer")]),
    with_args(cmd("client", -2, &["admin", "noscript"], 0, 0, 0, "A container for client connection commands.", "2.4.0", "connection"),
        &[arg("subcommand", "string")]),
    with_args(cmd("command", -1, &["loading", "stale"], 0, 0, 0, "Returns detailed information about all commands.", "2.8.13", "server"),
        &[Arg { optional: true, ..arg("subcommand", "string") }]),
    with_args(cmd("config", -2, &["admin", "noscript", "loading", "stale"], 0, 0, 0, "A container for server configuration commands.", "2.0.0", "server"),
        &[arg("subcommand", "string")]),
    with_args(cmd("info", -1, &["loading", "stale"], 0, 0, 0, "Returns information and statistics about the server.", "1.0.0", "server"),
        &[Arg { optional: true, multiple: true, ..arg("section", "string") }]),
    cmd("dbsize", 1, &["readonly", "fast"], 0, 0, 0, "Returns the number of keys in the database.", "1.0.0", "server"),
    with_complexity(cmd("flushall", -1, &["write"], 0, 0, 0, "Removes all keys from all databases.", "1.0.0", "server"), "O(N)"),
    with_complexity(cmd("flushdb", -1, &["write"], 0, 0, 0, "Removes all keys from the current database.", "1.0.0", "server"), "O(N)"),
    cmd("time", 1, &["loading", "stale", "fast"], 0, 0, 0, "Returns the server time.", "2.6.0", "server"),
    with_args(cmd("replicaof", 3, &["admin", "noscript", "stale"], 0, 0, 0, "Configures a server as a replica of, or promotes it to a master.", "5.0.0", "server"),
        &[arg("host", "string"), arg("port", "string")]),
    with_args(cmd("slaveof", 3, &["admin", "noscript", "stale"], 0, 0, 0, "Configures a server as a replica of another (deprecated alias of REPLICAOF).", "1.0.0", "server"),
        &[arg("host", "string"), arg("port", "string")]),
    with_args(cmd("shutdown", -1, &["admin", "noscript", "loading", "stale"], 0, 0, 0, "Shuts down the server.", "1.0.0", "server"),
        &[Arg { optional: true, ..arg("nosave-save", "string") }]),
    with_args(cmd("debug", -2, &["admin", "noscript", "loading", "stale"], 0, 0, 0, "A container for debugging commands.", "1.0.0", "server"),
        &[arg("subcommand", "string")]),

    // --- generic / keyspace ---
    with_complexity(with_args(cmd("del", -2, &["write"], 1, -1, 1, "Deletes one or more keys.", "1.0.0", "generic"), ARGS_KEYS), "O(N)"),
    with_complexity(with_args(cmd("unlink", -2, &["write", "fast"], 1, -1, 1, "Asynchronously deletes one or more keys.", "4.0.0", "generic"), ARGS_KEYS), "O(N)"),
    with_args(cmd("exists", -2, CF, 1, -1, 1, "Determines whether one or more keys exist.", "1.0.0", "generic"), ARGS_KEYS),
    with_args(cmd("type", 2, CF, 1, 1, 1, "Returns the type of value stored at a key.", "1.0.0", "generic"), ARGS_KEY),
    with_args(cmd("ttl", 2, CF, 1, 1, 1, "Returns the expiration time in seconds of a key.", "1.0.0", "generic"), ARGS_KEY),
    with_args(cmd("pttl", 2, CF, 1, 1, 1, "Returns the expiration time in milliseconds of a key.", "2.6.0", "generic"), ARGS_KEY),
    with_args(cmd("expire", -3, CWF, 1, 1, 1, "Sets a key's time to live in seconds.", "1.0.0", "generic"),
        &[A_KEY, arg("seconds", "integer"), Arg { optional: true, ..arg("condition", "oneof") }]),
    with_args(cmd("pexpire", -3, CWF, 1, 1, 1, "Sets a key's time to live in milliseconds.", "2.6.0", "generic"),
        &[A_KEY, arg("milliseconds", "integer")]),
    with_args(cmd("expireat", -3, CWF, 1, 1, 1, "Sets the expiration time of a key to a Unix timestamp (seconds).", "1.2.0", "generic"),
        &[A_KEY, arg("unix-time-seconds", "unix-time")]),
    with_args(cmd("pexpireat", -3, CWF, 1, 1, 1, "Sets the expiration time of a key to a Unix timestamp (milliseconds).", "2.6.0", "generic"),
        &[A_KEY, arg("unix-time-milliseconds", "unix-time")]),
    with_args(cmd("expiretime", 2, CF, 1, 1, 1, "Returns the Unix timestamp (seconds) at which a key expires.", "7.0.0", "generic"), ARGS_KEY),
    with_args(cmd("pexpiretime", 2, CF, 1, 1, 1, "Returns the Unix timestamp (milliseconds) at which a key expires.", "7.0.0", "generic"), ARGS_KEY),
    with_args(cmd("persist", 2, CWF, 1, 1, 1, "Removes the expiration from a key.", "2.2.0", "generic"), ARGS_KEY),
    with_complexity(with_args(cmd("eval", -3, CWF, 0, 0, 0, "Executes a server-side Lua script (atomic when all KEYS share a partition; use {hashtags}).", "2.6.0", "scripting"),
        &[arg("script", "string"), arg("numkeys", "integer"), Arg { optional: true, multiple: true, ..arg("key", "key") }, Arg { optional: true, multiple: true, ..arg("arg", "string") }]), "depends on script"),
    with_complexity(with_args(cmd("evalsha", -3, CWF, 0, 0, 0, "Executes a cached Lua script by SHA1.", "2.6.0", "scripting"),
        &[arg("sha1", "string"), arg("numkeys", "integer"), Arg { optional: true, multiple: true, ..arg("key", "key") }, Arg { optional: true, multiple: true, ..arg("arg", "string") }]), "depends on script"),
    with_args(cmd("script", -2, CRO, 0, 0, 0, "Manages the Lua script cache: LOAD | EXISTS | FLUSH.", "2.6.0", "scripting"),
        &[arg("subcommand", "oneof")]),
    with_complexity(with_args(cmd("pfadd", -2, CWF, 1, 1, 1, "Adds elements to a HyperLogLog key (probabilistic cardinality).", "2.8.9", "hyperloglog"),
        &[A_KEY, Arg { optional: true, multiple: true, ..arg("element", "string") }]), "O(1) per element"),
    with_complexity(with_args(cmd("pfcount", -2, CRO, 1, -1, 1, "Returns the approximate cardinality of HyperLogLog key(s).", "2.8.9", "hyperloglog"),
        &[Arg { multiple: true, ..arg("key", "key") }]), "O(registers)"),
    with_complexity(with_args(cmd("pfmerge", -2, CWF, 1, -1, 1, "Merges HyperLogLogs into a destination key.", "2.8.9", "hyperloglog"),
        &[arg("destkey", "key"), Arg { optional: true, multiple: true, ..arg("sourcekey", "key") }]), "O(registers)"),
    with_args(cmd("expiremember", -4, CWF, 1, 1, 1, "Sets a hash field / set member / zset member's time to live (KeyDB extension).", "7.0.0", "generic"),
        &[A_KEY, arg("member", "string"), arg("delay", "integer"), Arg { optional: true, ..arg("unit", "oneof") }]),
    with_args(cmd("expirememberat", 4, CWF, 1, 1, 1, "Sets a member's expiration to a Unix timestamp in seconds (KeyDB extension).", "7.0.0", "generic"),
        &[A_KEY, arg("member", "string"), arg("unix-time-seconds", "unix-time")]),
    with_args(cmd("pexpirememberat", 4, CWF, 1, 1, 1, "Sets a member's expiration to a Unix timestamp in milliseconds (KeyDB extension).", "7.0.0", "generic"),
        &[A_KEY, arg("member", "string"), arg("unix-time-milliseconds", "unix-time")]),
    with_complexity(with_args(cmd("keys", 2, CRO, 0, 0, 0, "Returns all keys matching a pattern.", "1.0.0", "generic"),
        &[arg("pattern", "pattern")]), "O(N)"),
    with_args(cmd("scan", -2, CRO, 0, 0, 0, "Iterates over the key names in the database.", "2.8.0", "generic"),
        &[arg("cursor", "integer"),
          Arg { token: Some("MATCH"), optional: true, ..arg("pattern", "pattern") },
          Arg { token: Some("COUNT"), optional: true, ..arg("count", "integer") },
          Arg { token: Some("TYPE"), optional: true, ..arg("type", "string") }]),
    cmd("randomkey", 1, CRO, 0, 0, 0, "Returns a random key from the database.", "1.0.0", "generic"),
    with_args(cmd("rename", 3, &["write"], 1, 2, 1, "Renames a key and overwrites the destination.", "1.0.0", "generic"),
        &[A_KEY, Arg { key_spec_index: Some(1), ..arg("newkey", "key") }]),
    with_args(cmd("renamenx", 3, CWF, 1, 2, 1, "Renames a key only when the target key name doesn't exist.", "1.0.0", "generic"),
        &[A_KEY, Arg { key_spec_index: Some(1), ..arg("newkey", "key") }]),
    with_args(cmd("copy", -3, CWF, 1, 2, 1, "Copies the value stored at the source key to the destination key.", "6.2.0", "generic"),
        &[A_KEY, Arg { key_spec_index: Some(1), ..arg("destination", "key") }, Arg { optional: true, multiple: true, args: COPY_OPTS, ..arg("options", "oneof") }]),
    with_args(cmd("object", -2, CRO, 2, 2, 1, "Returns static object introspection compatibility information.", "2.2.3", "generic"),
        OBJECT_ARGS),
    with_args(cmd("touch", -2, CF, 1, -1, 1, "Returns the number of existing keys out of those specified.", "3.2.1", "generic"), ARGS_KEYS),

    // --- strings ---
    with_args(cmd("get", 2, CF, 1, 1, 1, "Returns the string value of a key.", "1.0.0", "string"), ARGS_GET),
    with_args(cmd("set", -3, CW, 1, 1, 1, "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.", "1.0.0", "string"), ARGS_SET),
    with_args(cmd("setnx", 3, CWF, 1, 1, 1, "Sets the string value of a key only when the key doesn't exist.", "1.0.0", "string"), ARGS_KEY_VALUE),
    with_args(cmd("setex", 4, CW, 1, 1, 1, "Sets the string value and expiration time (seconds) of a key.", "2.0.0", "string"),
        &[A_KEY, arg("seconds", "integer"), arg("value", "string")]),
    with_args(cmd("psetex", 4, CW, 1, 1, 1, "Sets the string value and expiration time (milliseconds) of a key.", "2.6.0", "string"),
        &[A_KEY, arg("milliseconds", "integer"), arg("value", "string")]),
    with_args(cmd("getset", 3, CWF, 1, 1, 1, "Returns the previous string value of a key after setting it to a new value.", "1.0.0", "string"), ARGS_KEY_VALUE),
    with_args(cmd("getdel", 2, CWF, 1, 1, 1, "Returns the string value of a key after deleting the key.", "6.2.0", "string"), ARGS_KEY),
    with_args(cmd("getex", -2, CWF, 1, 1, 1, "Returns the string value of a key after setting its expiration time.", "6.2.0", "string"),
        &[A_KEY, Arg { optional: true, ..arg("expiration", "oneof") }]),
    with_args(cmd("append", 3, CW, 1, 1, 1, "Appends a string to the value of a key. Creates the key if it doesn't exist.", "2.0.0", "string"), ARGS_KEY_VALUE),
    with_args(cmd("strlen", 2, CF, 1, 1, 1, "Returns the length of a string value.", "2.2.0", "string"), ARGS_KEY),
    with_args(cmd("incr", 2, CWF, 1, 1, 1, "Increments the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.", "1.0.0", "string"), ARGS_KEY),
    with_args(cmd("decr", 2, CWF, 1, 1, 1, "Decrements the integer value of a key by one. Uses 0 as initial value if the key doesn't exist.", "1.0.0", "string"), ARGS_KEY),
    with_args(cmd("incrby", 3, CWF, 1, 1, 1, "Increments the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist.", "1.0.0", "string"),
        &[A_KEY, arg("increment", "integer")]),
    with_args(cmd("decrby", 3, CWF, 1, 1, 1, "Decrements the integer value of a key by a number. Uses 0 as initial value if the key doesn't exist.", "1.0.0", "string"),
        &[A_KEY, arg("decrement", "integer")]),
    with_args(cmd("incrbyfloat", 3, CWF, 1, 1, 1, "Increments the floating point value of a key by a number. Uses 0 as initial value if the key doesn't exist.", "2.6.0", "string"),
        &[A_KEY, arg("increment", "double")]),
    with_args(cmd("mget", -2, CF, 1, -1, 1, "Returns the string values of one or more keys.", "1.0.0", "string"), ARGS_KEYS),
    with_complexity(with_args(cmd("mset", -3, CW, 1, -1, 2, "Sets the string values of one or more keys.", "1.0.1", "string"),
        &[Arg { multiple: true, args: &[A_KEY, arg("value", "string")], ..arg("data", "block") }]), "O(N)"),
    with_complexity(with_args(cmd("msetnx", -3, CW, 1, -1, 2, "Sets the string values of one or more keys, only when none of them exist.", "1.0.1", "string"),
        &[Arg { multiple: true, args: &[A_KEY, arg("value", "string")], ..arg("data", "block") }]), "O(N)"),
    with_args(cmd("setrange", 4, CW, 1, 1, 1, "Overwrites part of a string value with another, extending it if needed.", "2.2.0", "string"),
        &[A_KEY, arg("offset", "integer"), arg("value", "string")]),
    with_args(cmd("getrange", 4, CRO, 1, 1, 1, "Returns a substring of the string stored at a key.", "2.4.0", "string"),
        &[A_KEY, arg("start", "integer"), arg("end", "integer")]),
    with_args(cmd("substr", 4, CRO, 1, 1, 1, "Returns a substring from a string value (deprecated alias of GETRANGE).", "1.0.0", "string"),
        &[A_KEY, arg("start", "integer"), arg("end", "integer")]),

    // --- hashes ---
    with_args(cmd("hset", -4, CWF, 1, 1, 1, "Creates or modifies the value of a field in a hash.", "2.0.0", "hash"), ARGS_HSET),
    with_args(cmd("hmset", -4, CWF, 1, 1, 1, "Sets the values of multiple fields (deprecated alias of HSET).", "2.0.0", "hash"), ARGS_HSET),
    with_args(cmd("hsetnx", 4, CWF, 1, 1, 1, "Sets the value of a field in a hash only when the field doesn't exist.", "2.0.0", "hash"),
        &[A_KEY, arg("field", "string"), arg("value", "string")]),
    with_args(cmd("hget", 3, CF, 1, 1, 1, "Returns the value of a field in a hash.", "2.0.0", "hash"), ARGS_KEY_FIELD),
    with_args(cmd("hmget", -3, CF, 1, 1, 1, "Returns the values of multiple fields in a hash.", "2.0.0", "hash"),
        &[A_KEY, Arg { multiple: true, ..arg("field", "string") }]),
    with_complexity(with_args(cmd("hgetall", 2, CRO, 1, 1, 1, "Returns all fields and values in a hash.", "2.0.0", "hash"), ARGS_KEY), "O(N)"),
    with_args(cmd("hdel", -3, CWF, 1, 1, 1, "Deletes one or more fields and their values from a hash. Deletes the hash if no fields remain.", "2.0.0", "hash"),
        &[A_KEY, Arg { multiple: true, ..arg("field", "string") }]),
    with_args(cmd("hgetdel", -4, CWF, 1, 1, 1, "Returns and deletes one or more hash fields.", "8.0.0", "hash"),
        &[A_KEY, Arg { args: HASH_FIELDS, ..arg("fields", "block") }]),
    with_args(cmd("hexpire", -6, CWF, 1, 1, 1, "Sets expiration on one or more hash fields in seconds.", "7.4.0", "hash"), HASH_EXPIRE_ARGS),
    with_args(cmd("hpexpire", -6, CWF, 1, 1, 1, "Sets expiration on one or more hash fields in milliseconds.", "7.4.0", "hash"), HASH_EXPIRE_ARGS),
    with_args(cmd("hexpireat", -6, CWF, 1, 1, 1, "Sets hash field expiration to a Unix timestamp in seconds.", "7.4.0", "hash"), HASH_EXPIRE_ARGS),
    with_args(cmd("hpexpireat", -6, CWF, 1, 1, 1, "Sets hash field expiration to a Unix timestamp in milliseconds.", "7.4.0", "hash"), HASH_EXPIRE_ARGS),
    with_args(cmd("httl", -4, CF, 1, 1, 1, "Returns hash field TTLs in seconds.", "7.4.0", "hash"), HASH_TTL_ARGS),
    with_args(cmd("hpttl", -4, CF, 1, 1, 1, "Returns hash field TTLs in milliseconds.", "7.4.0", "hash"), HASH_TTL_ARGS),
    with_args(cmd("hexpiretime", -4, CF, 1, 1, 1, "Returns hash field expiration timestamps in seconds.", "7.4.0", "hash"), HASH_TTL_ARGS),
    with_args(cmd("hpexpiretime", -4, CF, 1, 1, 1, "Returns hash field expiration timestamps in milliseconds.", "7.4.0", "hash"), HASH_TTL_ARGS),
    with_args(cmd("hpersist", -4, CWF, 1, 1, 1, "Removes expiration from one or more hash fields.", "7.4.0", "hash"), HASH_TTL_ARGS),
    with_args(cmd("hgetex", -5, CWF, 1, 1, 1, "Returns hash fields and optionally updates their expiration.", "8.0.0", "hash"), HGETEX_ARGS),
    with_args(cmd("hsetex", -6, CWF, 1, 1, 1, "Sets hash fields and optionally sets their expiration.", "8.0.0", "hash"), HSETEX_ARGS),
    with_args(cmd("hexists", 3, CF, 1, 1, 1, "Determines whether a field exists in a hash.", "2.0.0", "hash"), ARGS_KEY_FIELD),
    with_args(cmd("hlen", 2, CF, 1, 1, 1, "Returns the number of fields in a hash.", "2.0.0", "hash"), ARGS_KEY),
    with_complexity(with_args(cmd("hkeys", 2, CRO, 1, 1, 1, "Returns all fields in a hash.", "2.0.0", "hash"), ARGS_KEY), "O(N)"),
    with_complexity(with_args(cmd("hvals", 2, CRO, 1, 1, 1, "Returns all values in a hash.", "2.0.0", "hash"), ARGS_KEY), "O(N)"),
    with_args(cmd("hstrlen", 3, CF, 1, 1, 1, "Returns the length of the value of a field.", "3.2.0", "hash"), ARGS_KEY_FIELD),
    with_args(cmd("hincrby", 4, CWF, 1, 1, 1, "Increments the integer value of a field in a hash by a number. Uses 0 as initial value if the field doesn't exist.", "2.0.0", "hash"),
        &[A_KEY, arg("field", "string"), arg("increment", "integer")]),
    with_args(cmd("hincrbyfloat", 4, CWF, 1, 1, 1, "Increments the floating point value of a field by a number. Uses 0 as initial value if the field doesn't exist.", "2.6.0", "hash"),
        &[A_KEY, arg("field", "string"), arg("increment", "double")]),
    with_args(cmd("hrandfield", -2, CRO, 1, 1, 1, "Returns one or more random fields from a hash.", "6.2.0", "hash"),
        &[A_KEY, Arg { optional: true, ..arg("count", "integer") }]),
    with_args(cmd("hscan", -3, CRO, 1, 1, 1, "Iterates over fields and values of a hash.", "2.8.0", "hash"),
        &[A_KEY, arg("cursor", "integer")]),

    // --- sets ---
    with_args(cmd("sadd", -3, CWF, 1, 1, 1, "Adds one or more members to a set. Creates the key if it doesn't exist.", "1.0.0", "set"), ARGS_KEY_MEMBERS),
    with_args(cmd("srem", -3, CWF, 1, 1, 1, "Removes one or more members from a set. Deletes the set if the last member was removed.", "1.0.0", "set"), ARGS_KEY_MEMBERS),
    with_args(cmd("scard", 2, CF, 1, 1, 1, "Returns the number of members in a set.", "1.0.0", "set"), ARGS_KEY),
    with_args(cmd("sismember", 3, CF, 1, 1, 1, "Determines whether a member belongs to a set.", "1.0.0", "set"),
        &[A_KEY, arg("member", "string")]),
    with_args(cmd("smismember", -3, CF, 1, 1, 1, "Determines whether multiple members belong to a set.", "6.2.0", "set"), ARGS_KEY_MEMBERS),
    with_complexity(with_args(cmd("smembers", 2, CRO, 1, 1, 1, "Returns all members of a set.", "1.0.0", "set"), ARGS_KEY), "O(N)"),
    with_args(cmd("spop", -2, CWF, 1, 1, 1, "Returns one or more random members from a set after removing them. Deletes the set if the last member was popped.", "1.0.0", "set"),
        &[A_KEY, Arg { optional: true, ..arg("count", "integer") }]),
    with_args(cmd("srandmember", -2, CRO, 1, 1, 1, "Get one or multiple random members from a set.", "1.0.0", "set"),
        &[A_KEY, Arg { optional: true, ..arg("count", "integer") }]),
    with_args(cmd("sscan", -3, CRO, 1, 1, 1, "Iterates over members of a set.", "2.8.0", "set"),
        &[A_KEY, arg("cursor", "integer")]),
    with_args(cmd("smove", 4, CWF, 1, 2, 1, "Moves a member from one set to another.", "1.0.0", "set"),
        &[Arg { key_spec_index: Some(0), ..arg("source", "key") }, Arg { key_spec_index: Some(1), ..arg("destination", "key") }, arg("member", "string")]),
    with_complexity(with_args(cmd("sunion", -2, CRO, 1, -1, 1, "Returns the union of multiple sets.", "1.0.0", "set"), ARGS_KEYS), "O(N)"),
    with_complexity(with_args(cmd("sinter", -2, CRO, 1, -1, 1, "Returns the intersect of multiple sets.", "1.0.0", "set"), ARGS_KEYS), "O(N*M)"),
    with_complexity(with_args(cmd("sdiff", -2, CRO, 1, -1, 1, "Returns the difference of multiple sets.", "1.0.0", "set"), ARGS_KEYS), "O(N)"),
    with_complexity(with_args(cmd("sunionstore", -3, CW, 1, -1, 1, "Stores the union of multiple sets in a key.", "1.0.0", "set"),
        &[Arg { key_spec_index: Some(0), ..arg("destination", "key") }, Arg { multiple: true, key_spec_index: Some(1), ..arg("key", "key") }]), "O(N)"),
    with_complexity(with_args(cmd("sinterstore", -3, CW, 1, -1, 1, "Stores the intersect of multiple sets in a key.", "1.0.0", "set"),
        &[Arg { key_spec_index: Some(0), ..arg("destination", "key") }, Arg { multiple: true, key_spec_index: Some(1), ..arg("key", "key") }]), "O(N*M)"),
    with_complexity(with_args(cmd("sdiffstore", -3, CW, 1, -1, 1, "Stores the difference of multiple sets in a key.", "1.0.0", "set"),
        &[Arg { key_spec_index: Some(0), ..arg("destination", "key") }, Arg { multiple: true, key_spec_index: Some(1), ..arg("key", "key") }]), "O(N)"),
    with_complexity(with_args(cmd("sintercard", -3, CRO, 0, 0, 0, "Returns the number of members of the intersect of multiple sets.", "7.0.0", "set"),
        &[arg("numkeys", "integer"), Arg { multiple: true, ..arg("key", "key") },
          Arg { token: Some("LIMIT"), optional: true, ..arg("limit", "integer") }]), "O(N*M)"),

    // --- sorted sets ---
    with_args(cmd("zadd", -4, CWF, 1, 1, 1, "Adds one or more members to a sorted set, or updates their scores. Creates the key if it doesn't exist.", "1.2.0", "sorted_set"),
        &[A_KEY, Arg { optional: true, ..arg("condition", "oneof") },
          Arg { multiple: true, args: &[arg("score", "double"), arg("member", "string")], ..arg("data", "block") }]),
    with_args(cmd("zscore", 3, CF, 1, 1, 1, "Returns the score of a member in a sorted set.", "1.2.0", "sorted_set"),
        &[A_KEY, arg("member", "string")]),
    with_args(cmd("zmscore", -3, CF, 1, 1, 1, "Returns the scores of members in a sorted set.", "6.2.0", "sorted_set"),
        &[A_KEY, Arg { multiple: true, ..arg("member", "string") }]),
    with_args(cmd("zcard", 2, CF, 1, 1, 1, "Returns the number of members in a sorted set.", "1.2.0", "sorted_set"), ARGS_KEY),
    with_args(cmd("zincrby", 4, CWF, 1, 1, 1, "Increments the score of a member in a sorted set.", "1.2.0", "sorted_set"),
        &[A_KEY, arg("increment", "double"), arg("member", "string")]),
    with_args(cmd("zrem", -3, CWF, 1, 1, 1, "Removes one or more members from a sorted set. Deletes the sorted set if all members were removed.", "1.2.0", "sorted_set"),
        &[A_KEY, Arg { multiple: true, ..arg("member", "string") }]),
    with_complexity(with_args(cmd("zrange", -4, CRO, 1, 1, 1, "Returns members in a sorted set within a range of indexes.", "1.2.0", "sorted_set"),
        ZRANGE_ARGS), "O(N)"),
    with_complexity(with_args(cmd("zrangebyscore", -4, CRO, 1, 1, 1, "Returns members in a sorted set within a range of scores.", "1.0.5", "sorted_set"),
        &[A_KEY, arg("min", "double"), arg("max", "double")]), "O(log(N)+M)"),
    with_complexity(with_args(cmd("zrevrangebyscore", -4, CRO, 1, 1, 1, "Returns members in a sorted set within a range of scores in reverse order.", "2.2.0", "sorted_set"),
        &[A_KEY, arg("max", "double"), arg("min", "double")]), "O(log(N)+M)"),
    with_complexity(with_args(cmd("zrevrange", -4, CRO, 1, 1, 1, "Returns members in a sorted set within a range of indexes in reverse order.", "1.2.0", "sorted_set"),
        &[A_KEY, arg("start", "integer"), arg("stop", "integer")]), "O(log(N)+M)"),
    with_complexity(with_args(cmd("zrank", -3, CF, 1, 1, 1, "Returns the index of a member in a sorted set ordered by ascending scores.", "2.0.0", "sorted_set"),
        &[A_KEY, arg("member", "string")]), "O(log(N))"),
    with_complexity(with_args(cmd("zrevrank", -3, CF, 1, 1, 1, "Returns the index of a member in a sorted set ordered by descending scores.", "2.0.0", "sorted_set"),
        &[A_KEY, arg("member", "string")]), "O(log(N))"),
    with_complexity(with_args(cmd("zcount", 4, CF, 1, 1, 1, "Returns the count of members in a sorted set that have scores within a range.", "2.0.0", "sorted_set"),
        &[A_KEY, arg("min", "double"), arg("max", "double")]), "O(log(N))"),
    with_complexity(with_args(cmd("zlexcount", 4, CF, 1, 1, 1, "Returns the count of members in a sorted set within a lexicographical range.", "2.8.9", "sorted_set"),
        &[A_KEY, arg("min", "string"), arg("max", "string")]), "O(N)"),
    with_args(cmd("zrandmember", -2, CRO, 1, 1, 1, "Returns one or more members from a sorted set.", "6.2.0", "sorted_set"),
        &[A_KEY, Arg { optional: true, ..arg("count", "integer") }, Arg { token: Some("WITHSCORES"), optional: true, ..arg("withscores", "pure-token") }]),
    with_args(cmd("zrangestore", -5, CW, 1, 2, 1, "Stores a sorted-set range into a destination key.", "6.2.0", "sorted_set"),
        ZRANGESTORE_ARGS),
    with_args(cmd("zrangebylex", -4, CRO, 1, 1, 1, "Returns sorted-set members within a lexicographical range.", "2.8.9", "sorted_set"), ZLEX_RANGE_ARGS),
    with_args(cmd("zrevrangebylex", -4, CRO, 1, 1, 1, "Returns sorted-set members within a lexicographical range in reverse order.", "2.8.9", "sorted_set"), ZLEX_RANGE_ARGS),
    with_complexity(with_args(cmd("zpopmin", -2, CWF, 1, 1, 1, "Returns the lowest-scoring members from a sorted set after removing them. Deletes the sorted set if empty.", "5.0.0", "sorted_set"),
        &[A_KEY, Arg { optional: true, ..arg("count", "integer") }]), "O(log(N)*M)"),
    with_complexity(with_args(cmd("zpopmax", -2, CWF, 1, 1, 1, "Returns the highest-scoring members from a sorted set after removing them. Deletes the sorted set if empty.", "5.0.0", "sorted_set"),
        &[A_KEY, Arg { optional: true, ..arg("count", "integer") }]), "O(log(N)*M)"),
    with_args(cmd("bzpopmin", -3, &["write", "blocking", "fast"], 1, -2, 1, "Blocks until it can pop the lowest-scoring member from one sorted set.", "5.0.0", "sorted_set"), ARGS_KEYS),
    with_args(cmd("bzpopmax", -3, &["write", "blocking", "fast"], 1, -2, 1, "Blocks until it can pop the highest-scoring member from one sorted set.", "5.0.0", "sorted_set"), ARGS_KEYS),
    with_args(cmd("zmpop", -4, CWM, 0, 0, 0, "Pops members from the first non-empty sorted set.", "7.0.0", "sorted_set"), ZMPOP_ARGS),
    with_args(cmd("bzmpop", -5, CBM, 0, 0, 0, "Blocks until it can pop members from the first non-empty sorted set.", "7.0.0", "sorted_set"), BZMPOP_ARGS),
    with_complexity(with_args(cmd("zremrangebyscore", 4, CW, 1, 1, 1, "Removes members in a sorted set within a range of scores. Deletes the sorted set if empty.", "1.2.0", "sorted_set"),
        &[A_KEY, arg("min", "double"), arg("max", "double")]), "O(log(N)+M)"),
    with_args(cmd("zremrangebyrank", 4, CW, 1, 1, 1, "Removes members in a sorted set within a rank range.", "2.0.0", "sorted_set"),
        &[A_KEY, arg("start", "integer"), arg("stop", "integer")]),
    with_args(cmd("zremrangebylex", 4, CW, 1, 1, 1, "Removes members in a sorted set within a lexicographical range.", "2.8.9", "sorted_set"),
        &[A_KEY, arg("min", "string"), arg("max", "string")]),
    with_args(cmd("zunion", -3, CROM, 0, 0, 0, "Returns the union of multiple sorted sets.", "6.2.0", "sorted_set"), ZSETOP_ARGS),
    with_args(cmd("zinter", -3, CROM, 0, 0, 0, "Returns the intersection of multiple sorted sets.", "6.2.0", "sorted_set"), ZSETOP_ARGS),
    with_args(cmd("zdiff", -3, CROM, 0, 0, 0, "Returns the difference of multiple sorted sets.", "6.2.0", "sorted_set"), ZSETOP_ARGS),
    with_args(cmd("zunionstore", -4, CWM, 0, 0, 0, "Stores the union of multiple sorted sets.", "2.0.0", "sorted_set"), ZSETSTORE_ARGS),
    with_args(cmd("zinterstore", -4, CWM, 0, 0, 0, "Stores the intersection of multiple sorted sets.", "2.0.0", "sorted_set"), ZSETSTORE_ARGS),
    with_args(cmd("zdiffstore", -4, CWM, 0, 0, 0, "Stores the difference of multiple sorted sets.", "6.2.0", "sorted_set"), ZSETSTORE_ARGS),
    with_args(cmd("zintercard", -3, CROM, 0, 0, 0, "Returns the cardinality of a sorted-set intersection.", "7.0.0", "sorted_set"), ZINTERCARD_ARGS),
    with_args(cmd("zscan", -3, CRO, 1, 1, 1, "Iterates over members and scores of a sorted set.", "2.8.0", "sorted_set"),
        &[A_KEY, arg("cursor", "integer")]),

    // --- lists ---
    with_args(cmd("lpush", -3, CWF, 1, 1, 1, "Prepends one or more elements to a list. Creates the key if it doesn't exist.", "1.0.0", "list"), ARGS_PUSH),
    with_args(cmd("rpush", -3, CWF, 1, 1, 1, "Appends one or more elements to a list. Creates the key if it doesn't exist.", "1.0.0", "list"), ARGS_PUSH),
    with_args(cmd("lpushx", -3, CWF, 1, 1, 1, "Prepends one or more elements to a list only when the list exists.", "2.2.0", "list"), ARGS_PUSH),
    with_args(cmd("rpushx", -3, CWF, 1, 1, 1, "Appends one or more elements to a list only when the list exists.", "2.2.0", "list"), ARGS_PUSH),
    with_args(cmd("lpop", -2, CWF, 1, 1, 1, "Returns the first elements in a list after removing it. Deletes the list if the last element was popped.", "1.0.0", "list"),
        &[A_KEY, Arg { optional: true, ..arg("count", "integer") }]),
    with_args(cmd("rpop", -2, CWF, 1, 1, 1, "Returns and removes the last elements of a list. Deletes the list if the last element was popped.", "1.0.0", "list"),
        &[A_KEY, Arg { optional: true, ..arg("count", "integer") }]),
    with_args(cmd("llen", 2, CF, 1, 1, 1, "Returns the length of a list.", "1.0.0", "list"), ARGS_KEY),
    with_complexity(with_args(cmd("lrange", 4, CRO, 1, 1, 1, "Returns a range of elements from a list.", "1.0.0", "list"),
        &[A_KEY, arg("start", "integer"), arg("stop", "integer")]), "O(S+N)"),
    with_args(cmd("lindex", 3, CRO, 1, 1, 1, "Returns an element from a list by its index.", "1.0.0", "list"),
        &[A_KEY, arg("index", "integer")]),
    with_args(cmd("lset", 4, CW, 1, 1, 1, "Sets the value of an element in a list by its index.", "1.0.0", "list"),
        &[A_KEY, arg("index", "integer"), arg("element", "string")]),
    with_complexity(with_args(cmd("lrem", 4, &["write"], 1, 1, 1, "Removes elements from a list. Deletes the list if the last element was removed.", "1.0.0", "list"),
        &[A_KEY, arg("count", "integer"), arg("element", "string")]), "O(N)"),
    with_complexity(with_args(cmd("ltrim", 4, &["write"], 1, 1, 1, "Removes elements from both ends a list. Deletes the list if all elements were trimmed.", "1.0.0", "list"),
        &[A_KEY, arg("start", "integer"), arg("stop", "integer")]), "O(N)"),
    with_complexity(with_args(cmd("linsert", 5, CW, 1, 1, 1, "Inserts an element before or after another element in a list.", "2.2.0", "list"),
        &[A_KEY, arg("where", "oneof"), arg("pivot", "string"), arg("element", "string")]), "O(N)"),
    with_complexity(with_args(cmd("lpos", -3, CRO, 1, 1, 1, "Returns the index of matching elements in a list.", "6.0.6", "list"),
        &[A_KEY, arg("element", "string"),
          Arg { token: Some("RANK"), optional: true, ..arg("rank", "integer") },
          Arg { token: Some("COUNT"), optional: true, ..arg("num-matches", "integer") }]), "O(N)"),
    with_args(cmd("lmove", 5, CW, 1, 2, 1, "Returns an element after popping it from one list and pushing it to another. Deletes the list if the last element was moved.", "6.2.0", "list"),
        &[Arg { key_spec_index: Some(0), ..arg("source", "key") }, Arg { key_spec_index: Some(1), ..arg("destination", "key") },
          arg("wherefrom", "oneof"), arg("whereto", "oneof")]),
    with_args(cmd("rpoplpush", 3, CW, 1, 2, 1, "Returns the last element of a list after removing and pushing it to another list. Deletes the list if the last element was moved.", "1.2.0", "list"),
        &[Arg { key_spec_index: Some(0), ..arg("source", "key") }, Arg { key_spec_index: Some(1), ..arg("destination", "key") }]),
    with_args(cmd("lmpop", -4, CWM, 0, 0, 0, "Returns multiple elements from the first non-empty list.", "7.0.0", "list"), LMPOP_ARGS),
    with_args(cmd("blpop", -3, &["write", "blocking", "fast"], 1, -2, 1, "Removes and returns the first element in a list, or blocks until one is available.", "2.0.0", "list"),
        &[Arg { multiple: true, ..A_KEY }, arg("timeout", "double")]),
    with_args(cmd("brpop", -3, &["write", "blocking", "fast"], 1, -2, 1, "Removes and returns the last element in a list, or blocks until one is available.", "2.0.0", "list"),
        &[Arg { multiple: true, ..A_KEY }, arg("timeout", "double")]),
    with_args(cmd("blmove", 6, &["write", "denyoom", "blocking"], 1, 2, 1, "Pops an element from a list, pushes it to another list and returns it. Blocks until an element is available otherwise.", "6.2.0", "list"),
        &[Arg { key_spec_index: Some(0), ..arg("source", "key") }, Arg { key_spec_index: Some(1), ..arg("destination", "key") },
          arg("wherefrom", "oneof"), arg("whereto", "oneof"), arg("timeout", "double")]),
    with_args(cmd("brpoplpush", 4, &["write", "denyoom", "blocking"], 1, 2, 1, "Pops an element from a list, pushes it to another list and returns it. Block until an element is available otherwise.", "2.2.0", "list"),
        &[Arg { key_spec_index: Some(0), ..arg("source", "key") }, Arg { key_spec_index: Some(1), ..arg("destination", "key") }, arg("timeout", "double")]),
    with_args(cmd("blmpop", -5, CBM, 0, 0, 0, "Blocks until it can return multiple elements from the first non-empty list.", "7.0.0", "list"), BLMPOP_ARGS),

    // --- streams ---
    with_args(cmd("xadd", -5, CWF, 1, 1, 1, "Appends a new message to a stream. Creates the key if it doesn't exist.", "5.0.0", "stream"),
        &[A_KEY, arg("id-or-auto", "string"),
          Arg { multiple: true, args: &[arg("field", "string"), arg("value", "string")], ..arg("data", "block") }]),
    with_args(cmd("xlen", 2, CF, 1, 1, 1, "Returns the number of messages in a stream.", "5.0.0", "stream"), ARGS_KEY),
    with_complexity(with_args(cmd("xrange", -4, CRO, 1, 1, 1, "Returns the messages from a stream within a range of IDs.", "5.0.0", "stream"),
        &[A_KEY, arg("start", "string"), arg("end", "string"),
          Arg { token: Some("COUNT"), optional: true, ..arg("count", "integer") }]), "O(N)"),
    with_complexity(with_args(cmd("xrevrange", -4, CRO, 1, 1, 1, "Returns the messages from a stream within a range of IDs in reverse order.", "5.0.0", "stream"),
        &[A_KEY, arg("end", "string"), arg("start", "string"),
          Arg { token: Some("COUNT"), optional: true, ..arg("count", "integer") }]), "O(N)"),
    with_args(cmd("xread", -4, &["readonly", "blocking", "movablekeys"], 0, 0, 0, "Returns messages from multiple streams with IDs greater than the ones requested. Blocks until a message is available otherwise.", "5.0.0", "stream"),
        &[Arg { token: Some("COUNT"), optional: true, ..arg("count", "integer") },
          Arg { token: Some("BLOCK"), optional: true, ..arg("milliseconds", "integer") },
          arg("streams", "pure-token")]),
    with_args(cmd("xdel", -3, CWF, 1, 1, 1, "Returns the number of messages after removing them from a stream.", "5.0.0", "stream"),
        &[A_KEY, Arg { multiple: true, ..arg("id", "string") }]),
    with_complexity(with_args(cmd("xtrim", -4, &["write"], 1, 1, 1, "Deletes messages from the beginning of a stream.", "5.0.0", "stream"),
        &[A_KEY, arg("strategy", "oneof"), arg("threshold", "string")]), "O(N)"),
    with_args(cmd("xsetid", -3, CWF, 1, 1, 1, "Sets a stream's last-generated id metadata.", "5.0.0", "stream"), XSETID_ARGS),
    with_args(cmd("xinfo", -2, CRO, 0, 0, 0, "Returns stream introspection information.", "5.0.0", "stream"), XINFO_ARGS),

    // --- pub/sub ---
    with_args(cmd("subscribe", -2, &["pubsub", "loading", "stale", "fast"], 0, 0, 0, "Listens for messages published to channels.", "2.0.0", "pubsub"),
        &[Arg { multiple: true, ..arg("channel", "string") }]),
    with_args(cmd("unsubscribe", -1, &["pubsub", "loading", "stale", "fast"], 0, 0, 0, "Stops listening to messages posted to channels.", "2.0.0", "pubsub"),
        &[Arg { optional: true, multiple: true, ..arg("channel", "string") }]),
    with_args(cmd("psubscribe", -2, &["pubsub", "loading", "stale", "fast"], 0, 0, 0, "Listens for messages published to channels that match one or more patterns.", "2.0.0", "pubsub"),
        &[Arg { multiple: true, ..arg("pattern", "pattern") }]),
    with_args(cmd("punsubscribe", -1, &["pubsub", "loading", "stale", "fast"], 0, 0, 0, "Stops listening to messages published to channels that match one or more patterns.", "2.0.0", "pubsub"),
        &[Arg { optional: true, multiple: true, ..arg("pattern", "pattern") }]),
    with_args(cmd("publish", 3, &["pubsub", "loading", "stale", "fast"], 0, 0, 0, "Posts a message to a channel.", "2.0.0", "pubsub"),
        &[arg("channel", "string"), arg("message", "string")]),
    with_args(cmd("pubsub", -2, &["pubsub", "loading", "stale", "fast"], 0, 0, 0, "A container for Pub/Sub commands.", "2.8.0", "pubsub"),
        &[arg("subcommand", "string")]),

    // --- transactions ---
    cmd("multi", 1, &["loading", "stale", "fast"], 0, 0, 0, "Starts a transaction.", "1.2.0", "transactions"),
    cmd("exec", 1, &["loading", "stale"], 0, 0, 0, "Executes all commands in a transaction.", "1.2.0", "transactions"),
    cmd("discard", 1, &["loading", "stale", "fast"], 0, 0, 0, "Discards a transaction.", "2.0.0", "transactions"),
    with_args(cmd("watch", -2, &["loading", "stale", "fast"], 1, -1, 1, "Monitors changes to keys to determine the execution of a transaction (unsupported in marekvs; AP store has no CAS).", "2.2.0", "transactions"), ARGS_KEYS),
    cmd("unwatch", 1, &["loading", "stale", "fast"], 0, 0, 0, "Forgets about watched keys of a transaction.", "2.2.0", "transactions"),

    // --- budgets (BG.*, marekvs extension, design/13) ---
    with_args(cmd("bg.create", -3, CW, 1, 1, 1, "Creates (or regenerates) a distributed budget with escrow split across nodes.", "1.2.0", "budget"),
        &[A_KEY, arg("capacity", "integer"),
          Arg { token: Some("MODE"), optional: true, ..arg("mode", "string") },
          Arg { token: Some("TTL"), optional: true, ..arg("default-ttl-ms", "integer") },
          Arg { token: Some("MAXTTL"), optional: true, ..arg("max-ttl-ms", "integer") },
          Arg { token: Some("MAXAMOUNT"), optional: true, ..arg("max-amount", "integer") },
          Arg { token: Some("NODES"), optional: true, multiple: true, ..arg("node", "integer") },
          Arg { token: Some("SEQ"), optional: true, ..arg("op-seq", "integer") }]),
    with_args(cmd("bg.topup", -3, CWF, 1, 1, 1, "Adds capacity to a budget (central actor; idempotent with SEQ).", "1.2.0", "budget"),
        &[A_KEY, arg("amount", "integer"),
          Arg { token: Some("NODE"), optional: true, ..arg("node", "integer") },
          Arg { token: Some("SEQ"), optional: true, ..arg("op-seq", "integer") }]),
    with_args(cmd("bg.reserve", -3, CW, 1, 1, 1, "Reserves an amount from the budget, returning a token with a deadline; never overspends (fails closed).", "1.2.0", "budget"),
        &[A_KEY, arg("amount", "integer"),
          Arg { token: Some("TTL"), optional: true, ..arg("ttl-ms", "integer") },
          Arg { token: Some("REQID"), optional: true, ..arg("reqid", "integer") }]),
    with_args(cmd("bg.commit", -3, CWF, 1, 1, 1, "Reports a token's final spend and returns the unspent remainder to the budget.", "1.2.0", "budget"),
        &[A_KEY, arg("token", "string"), Arg { optional: true, ..arg("spent", "integer") }]),
    with_args(cmd("bg.release", 3, CWF, 1, 1, 1, "Returns a token's undrawn reservation to the budget.", "1.2.0", "budget"),
        &[A_KEY, arg("token", "string")]),
    with_args(cmd("bg.draw", 4, CWF, 1, 1, 1, "Draws an incremental, server-tracked amount against a reservation token.", "1.2.0", "budget"),
        &[A_KEY, arg("token", "string"), arg("amount", "integer")]),
    with_args(cmd("bg.info", 2, CRO, 1, 1, 1, "Returns a node-local view of a budget's configuration and escrow ledgers.", "1.2.0", "budget"),
        ARGS_KEY),
];

/// The full command catalog.
pub fn all() -> &'static [CommandDoc] {
    TABLE
}

/// Look up a command by name, case-insensitively.
pub fn find(name: &str) -> Option<&'static CommandDoc> {
    TABLE.iter().find(|d| d.name.eq_ignore_ascii_case(name))
}

/// Classic `COMMAND INFO` 6-tuple: `[name, arity, [flags], first, last, step]`.
pub fn info_entry(d: &CommandDoc) -> Reply {
    Reply::Array(vec![
        Reply::bulk_str(d.name),
        Reply::Int(d.arity),
        Reply::Array(d.flags.iter().map(|f| Reply::Simple(f)).collect()),
        Reply::Int(d.first_key),
        Reply::Int(d.last_key),
        Reply::Int(d.step),
    ])
}

/// The `COMMAND DOCS` value map for one command (`summary`, `since`, `group`,
/// `complexity`, `arity`, `arguments`). Keyed by the command name in the
/// caller's outer map.
pub fn docs_value(d: &CommandDoc) -> Reply {
    let mut m = vec![
        (Reply::bulk_str("summary"), Reply::bulk_str(d.summary)),
        (Reply::bulk_str("since"), Reply::bulk_str(d.since)),
        (Reply::bulk_str("group"), Reply::bulk_str(d.group)),
        (Reply::bulk_str("complexity"), Reply::bulk_str(d.complexity)),
        (Reply::bulk_str("arity"), Reply::Int(d.arity)),
    ];
    if !d.args.is_empty() {
        m.push((
            Reply::bulk_str("arguments"),
            Reply::Array(d.args.iter().map(arg_reply).collect()),
        ));
    }
    Reply::Map(m)
}

/// Serialize one argument spec, matching real redis field order/shape.
fn arg_reply(a: &Arg) -> Reply {
    let container = matches!(a.typ, "oneof" | "block");
    let mut m = vec![
        (Reply::bulk_str("name"), Reply::bulk_str(a.name)),
        (Reply::bulk_str("type"), Reply::bulk_str(a.typ)),
    ];
    // Leaf args carry display_text; containers (oneof/block) do not.
    if !container {
        m.push((Reply::bulk_str("display_text"), Reply::bulk_str(a.name)));
    }
    if let Some(k) = a.key_spec_index {
        m.push((Reply::bulk_str("key_spec_index"), Reply::Int(k)));
    }
    if let Some(t) = a.token {
        m.push((Reply::bulk_str("token"), Reply::bulk_str(t)));
    }
    if let Some(s) = a.since {
        m.push((Reply::bulk_str("since"), Reply::bulk_str(s)));
    }
    let mut flags = Vec::new();
    if a.optional {
        flags.push(Reply::Simple("optional"));
    }
    if a.multiple {
        flags.push(Reply::Simple("multiple"));
    }
    if !flags.is_empty() {
        m.push((Reply::bulk_str("flags"), Reply::Array(flags)));
    }
    if !a.args.is_empty() {
        m.push((
            Reply::bulk_str("arguments"),
            Reply::Array(a.args.iter().map(arg_reply).collect()),
        ));
    }
    Reply::Map(m)
}

/// Raw key extraction via the first/last/step spec — shared by
/// COMMAND GETKEYS and the script bridge's declared-key enforcement.
pub fn extract_keys<'a>(d: &CommandDoc, argv: &'a [Vec<u8>]) -> Vec<&'a [u8]> {
    if d.first_key == 0 {
        return Vec::new();
    }
    let argc = argv.len() as i64;
    let last = if d.last_key < 0 {
        argc + d.last_key
    } else {
        d.last_key
    };
    let step = if d.step <= 0 { 1 } else { d.step };
    let mut keys = Vec::new();
    let mut i = d.first_key;
    while i <= last && (i as usize) < argv.len() {
        keys.push(argv[i as usize].as_slice());
        i += step;
    }
    keys
}

/// `COMMAND GETKEYS <cmd> <args...>`: extract key arguments from `argv`, where
/// `argv[0]` is the target command name. Uses the table's first/last/step key
/// spec — enough for the fixed-position commands we serve; movable-key layouts
/// (e.g. SINTERCARD's `numkeys`) are reported as unextractable.
pub fn getkeys(argv: &[Vec<u8>]) -> Reply {
    let Some(name) = argv.first() else {
        return Reply::err("ERR Unknown command name");
    };
    let name = String::from_utf8_lossy(name);
    let Some(d) = find(&name) else {
        return Reply::err("ERR Invalid command specified");
    };
    let argc = argv.len() as i64;
    // Arity check (negative = "at least |arity|").
    let arity_ok = if d.arity < 0 {
        argc >= -d.arity
    } else {
        argc == d.arity
    };
    if !arity_ok {
        return Reply::err("ERR Invalid number of arguments specified for command");
    }
    if d.first_key == 0 {
        return Reply::err("ERR The command has no key arguments");
    }
    let last = if d.last_key < 0 {
        argc + d.last_key
    } else {
        d.last_key
    };
    let step = if d.step <= 0 { 1 } else { d.step };
    let mut keys = Vec::new();
    let mut i = d.first_key;
    while i <= last && (i as usize) < argv.len() {
        keys.push(Reply::Bulk(argv[i as usize].clone()));
        i += step;
    }
    if keys.is_empty() {
        return Reply::err("ERR Invalid arguments specified for command");
    }
    Reply::Array(keys)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn catalog_names_are_unique() {
        let mut seen = HashSet::new();
        for d in all() {
            assert!(
                seen.insert(d.name),
                "duplicate COMMAND DOCS entry for {}",
                d.name
            );
        }
    }

    #[test]
    fn newly_added_commands_have_argument_docs() {
        for name in [
            "copy",
            "object",
            "hgetdel",
            "hexpire",
            "hpexpire",
            "hexpireat",
            "hpexpireat",
            "httl",
            "hpttl",
            "hexpiretime",
            "hpexpiretime",
            "hpersist",
            "hgetex",
            "hsetex",
            "zlexcount",
            "bzpopmin",
            "bzpopmax",
            "zmpop",
            "bzmpop",
            "zrandmember",
            "zrangestore",
            "zrangebylex",
            "zrevrangebylex",
            "zremrangebyrank",
            "zremrangebylex",
            "zunion",
            "zinter",
            "zdiff",
            "zunionstore",
            "zinterstore",
            "zdiffstore",
            "zintercard",
            "lmpop",
            "blmpop",
            "xsetid",
            "xinfo",
            "bg.create",
            "bg.topup",
            "bg.reserve",
            "bg.commit",
            "bg.release",
            "bg.draw",
            "bg.info",
        ] {
            let doc = find(name).unwrap_or_else(|| panic!("missing COMMAND DOCS entry for {name}"));
            assert!(!doc.args.is_empty(), "{name} should document its arguments");
            let _ = docs_value(doc);
        }
    }
}
