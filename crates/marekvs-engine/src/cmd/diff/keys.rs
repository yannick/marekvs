//! Strict DIFF key grammar and the client immutable namespace fence.
use crate::{reply::Reply, Engine};
use marekvs_diff::Sid;
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffKey {
    Branch { tag: String, name: String },
    Snapshot { tag: String, name: String },
    Graph { tag: String, name: String },
    Decision { tag: String, name: String },
    Result { tag: String, name: String },
}
impl DiffKey {
    pub fn parse(key: &[u8]) -> Result<Self, Reply> {
        let bad = || Reply::err("DIFFKEY malformed DIFF key");
        let key = std::str::from_utf8(key).map_err(|_| bad())?;
        let (family, tail) = key.split_once(":{").ok_or_else(bad)?;
        let (tag, tail) = tail.split_once("}:").ok_or_else(bad)?;
        let (kind, name) = tail.split_once(':').ok_or_else(bad)?;
        if tag.is_empty()
            || name.is_empty()
            || tag.contains(['{', '}'])
            || tail.contains(['{', '}'])
        {
            return Err(bad());
        }
        if matches!(kind, "s" | "g" | "d")
            && (name.len() != 32
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
        {
            return Err(bad());
        }
        let (tag, name) = (tag.to_owned(), name.to_owned());
        Ok(match (family, kind) {
            ("doc", "b") => Self::Branch { tag, name },
            ("doc", "s") => Self::Snapshot { tag, name },
            ("diff", "g") => Self::Graph { tag, name },
            ("diff", "d") => Self::Decision { tag, name },
            ("diff", "r") => Self::Result { tag, name },
            _ => return Err(bad()),
        })
    }
    pub fn tag(&self) -> &str {
        match self {
            Self::Branch { tag, .. }
            | Self::Snapshot { tag, .. }
            | Self::Graph { tag, .. }
            | Self::Decision { tag, .. }
            | Self::Result { tag, .. } => tag,
        }
    }
    pub fn name(&self) -> &str {
        match self {
            Self::Branch { name, .. }
            | Self::Snapshot { name, .. }
            | Self::Graph { name, .. }
            | Self::Decision { name, .. }
            | Self::Result { name, .. } => name,
        }
    }
}
pub fn snapshot_key(tag: &str, sid: Sid) -> Vec<u8> {
    format!("doc:{{{tag}}}:s:{:032x}", sid.0).into_bytes()
}
pub fn graph_key(tag: &str, id: &str) -> Vec<u8> {
    format!("diff:{{{tag}}}:g:{id}").into_bytes()
}
pub fn decision_key(tag: &str, id: &str) -> Vec<u8> {
    format!("diff:{{{tag}}}:d:{id}").into_bytes()
}
pub fn result_key(tag: &str, token: &str) -> Vec<u8> {
    format!("diff:{{{tag}}}:r:{token}").into_bytes()
}
/// Reserve the prefix even if a client supplied a malformed digest.
pub fn is_immutable_key(key: &[u8]) -> bool {
    let (family, tail) = if let Some(t) = key.strip_prefix(b"doc:{") {
        (false, t)
    } else if let Some(t) = key.strip_prefix(b"diff:{") {
        (true, t)
    } else {
        return false;
    };
    tail.windows(2).position(|p| p == b"}:").is_some_and(|i| {
        let suffix = &tail[i + 2..];
        if family {
            suffix.starts_with(b"g:") || suffix.starts_with(b"r:")
        } else {
            suffix.starts_with(b"s:")
        }
    })
}
tokio::task_local! { pub(crate) static INTERNAL: bool; }

pub fn guard<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> Result<(), Reply> {
    if INTERNAL.try_with(|internal| *internal).unwrap_or(false) {
        return Ok(());
    }
    if keys.into_iter().any(is_immutable_key) {
        Err(Reply::err(
            "DIFFIMMUTABLE immutable snapshot, graph or result key",
        ))
    } else {
        Ok(())
    }
}
/// Validate the entire mutation set before the first write. Sources that are
/// only read (COPY, set algebra stores, PFMERGE) remain usable snapshots.
pub fn guard_command(name: &str, args: &[Vec<u8>]) -> Result<(), Reply> {
    if !Engine::is_write_command(name)
        || name.starts_with("DIFF.")
        || matches!(name, "EVAL" | "EVALSHA" | "FLUSHDB" | "FLUSHALL")
    {
        return Ok(());
    }
    let single = |i: usize| guard(args.get(i).map(Vec::as_slice));
    match name {
        "COPY" => single(2),
        "SUNIONSTORE" | "SINTERSTORE" | "SDIFFSTORE" | "ZUNIONSTORE" | "ZINTERSTORE"
        | "ZDIFFSTORE" | "ZRANGESTORE" | "PFMERGE" => single(1),
        "LMPOP" | "ZMPOP" | "BLMPOP" | "BZMPOP" => {
            let n = if name.starts_with('B') { 2 } else { 1 };
            let count = args
                .get(n)
                .and_then(|v| std::str::from_utf8(v).ok())
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(0);
            guard(args.iter().skip(n + 1).take(count).map(Vec::as_slice))
        }
        _ => {
            if let Some(doc) = super::super::command_docs::find(name) {
                guard(super::super::command_docs::extract_keys(doc, args))
            } else {
                Ok(())
            }
        }
    }
}
