//! PROTO.* error surface (design/17): raw-code errors in the
//! BUDGETEXHAUSTED style so clients can dispatch on the first token.

use crate::reply::Reply;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtoErr {
    /// Schema (or schema version) not present in the registry.
    NoSchema(String),
    /// Schema upload/compile problem (parse error, missing import, limits).
    Schema(String),
    /// Value failed validation against the resolved message type.
    Validate(String),
    /// No explicit TYPE argument and no prefix binding covers the key.
    NoBinding,
    /// Field path syntax/resolution error.
    Path(String),
    /// Key holds a non-proto value.
    WrongType,
    /// Anything else (already fully formatted).
    Other(String),
}

impl ProtoErr {
    pub fn reply(&self) -> Reply {
        match self {
            ProtoErr::NoSchema(what) => Reply::err(format!("NOSCHEMA {what}")),
            ProtoErr::Schema(what) => Reply::err(format!("SCHEMAERR {what}")),
            ProtoErr::Validate(what) => Reply::err(format!("PROTOVALIDATE {what}")),
            ProtoErr::NoBinding => Reply::err(
                "NOBINDING no TYPE given and no prefix binding covers the key \
                 (PROTO.BIND a prefix or pass TYPE)",
            ),
            ProtoErr::Path(what) => Reply::err(format!("PROTOPATH {what}")),
            ProtoErr::WrongType => Reply::wrongtype(),
            ProtoErr::Other(msg) => Reply::err(msg.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_codes() {
        assert!(matches!(
            ProtoErr::NoSchema("x".into()).reply(),
            Reply::Err(e) if e.starts_with("NOSCHEMA ")
        ));
        assert!(matches!(
            ProtoErr::Schema("bad".into()).reply(),
            Reply::Err(e) if e.starts_with("SCHEMAERR ")
        ));
        assert!(matches!(
            ProtoErr::Validate("bad".into()).reply(),
            Reply::Err(e) if e.starts_with("PROTOVALIDATE ")
        ));
        assert!(matches!(
            ProtoErr::NoBinding.reply(),
            Reply::Err(e) if e.starts_with("NOBINDING ")
        ));
        assert!(matches!(
            ProtoErr::Path("depth".into()).reply(),
            Reply::Err(e) if e.starts_with("PROTOPATH ")
        ));
        assert!(matches!(
            ProtoErr::WrongType.reply(),
            Reply::Err(e) if e.starts_with("WRONGTYPE ")
        ));
    }
}
