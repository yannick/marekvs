//! Command results, decoupled from the wire: handlers build `Reply`, the
//! connection layer serializes it into a RESP2/RESP3 `ReplyBuf`.

use marekvs_resp::ReplyBuf;

#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    Simple(&'static str),
    SimpleOwned(String),
    Err(String),
    Int(i64),
    Bulk(Vec<u8>),
    Null,
    NullArray,
    Array(Vec<Reply>),
    Map(Vec<(Reply, Reply)>),
    Set(Vec<Reply>),
    Double(f64),
    Bool(bool),
    Verbatim(String),
    /// Nothing is written (e.g. handled out-of-band, like SUBSCRIBE frames).
    None,
}

impl Reply {
    pub fn ok() -> Reply {
        Reply::Simple("OK")
    }

    pub fn err(msg: impl Into<String>) -> Reply {
        Reply::Err(msg.into())
    }

    pub fn wrongtype() -> Reply {
        Reply::Err("WRONGTYPE Operation against a key holding the wrong kind of value".into())
    }

    pub fn wrong_args(cmd: &str) -> Reply {
        Reply::Err(format!(
            "ERR wrong number of arguments for '{}' command",
            cmd.to_lowercase()
        ))
    }

    pub fn not_int() -> Reply {
        Reply::Err("ERR value is not an integer or out of range".into())
    }

    pub fn not_float() -> Reply {
        Reply::Err("ERR value is not a valid float".into())
    }

    pub fn syntax() -> Reply {
        Reply::Err("ERR syntax error".into())
    }

    pub fn bulk_str(s: impl Into<String>) -> Reply {
        Reply::Bulk(s.into().into_bytes())
    }

    pub fn write(self, out: &mut ReplyBuf) {
        match self {
            Reply::Simple(s) => out.simple(s),
            Reply::SimpleOwned(s) => out.simple(&s),
            Reply::Err(e) => out.error(&e),
            Reply::Int(i) => out.int(i),
            Reply::Bulk(b) => out.bulk(&b),
            Reply::Null => out.null(),
            Reply::NullArray => out.null_array(),
            Reply::Array(items) => {
                out.array(items.len());
                for item in items {
                    item.write(out);
                }
            }
            Reply::Map(pairs) => {
                out.map(pairs.len());
                for (k, v) in pairs {
                    k.write(out);
                    v.write(out);
                }
            }
            Reply::Set(items) => {
                out.set(items.len());
                for item in items {
                    item.write(out);
                }
            }
            Reply::Double(f) => out.double(f),
            Reply::Bool(b) => out.boolean(b),
            Reply::Verbatim(s) => out.verbatim(&s),
            Reply::None => {}
        }
    }
}
