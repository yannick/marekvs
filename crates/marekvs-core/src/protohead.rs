//! Head-tail codec for protobuf typed values (`head::CTYPE_PROTO`, design/17).
//!
//! A proto value is a HEAD-ONLY record: the collection-head payload is the
//! standard `[ctype u8][del_hlc u64 BE]` prefix followed by this tail:
//!
//! ```text
//! offset size        field
//! 0      1           fmt: codec version, currently 1
//! 1      4           schema_version (big-endian u32)
//! 5      varint      nlen — schema name length
//! …      nlen        schema name (utf-8)
//! …      varint      tlen — fully-qualified message type name length
//! …      tlen        fq type name (utf-8)
//! …      rest        encoded protobuf message bytes
//! ```
//!
//! Whole-message LWW rides the existing head merge; this module is plain
//! bytes — no protobuf dependency (compilation/reflection live in the
//! engine's `proto` module).

pub const FMT_V1: u8 = 1;

/// Decoded view of a proto head tail (borrows from the payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtoHead<'a> {
    /// Registry version of the schema the message was validated against.
    pub schema_version: u32,
    /// Registry schema name.
    pub schema: &'a str,
    /// Fully-qualified protobuf message type name (no leading dot).
    pub type_name: &'a str,
    /// The encoded protobuf message.
    pub msg: &'a [u8],
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let b = (v & 0x7F) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            return;
        }
        out.push(b | 0x80);
    }
}

fn get_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut v = 0u64;
    let mut shift = 0;
    for (i, &b) in buf.iter().enumerate() {
        v |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            return Some((v, i + 1));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}

/// Encode a proto head tail.
pub fn encode(schema: &str, schema_version: u32, type_name: &str, msg: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 4 + 2 + schema.len() + 2 + type_name.len() + msg.len());
    out.push(FMT_V1);
    out.extend_from_slice(&schema_version.to_be_bytes());
    put_varint(&mut out, schema.len() as u64);
    out.extend_from_slice(schema.as_bytes());
    put_varint(&mut out, type_name.len() as u64);
    out.extend_from_slice(type_name.as_bytes());
    out.extend_from_slice(msg);
    out
}

/// Decode a proto head tail. `None` on unknown fmt, truncation, or non-utf8
/// names.
pub fn decode(tail: &[u8]) -> Option<ProtoHead<'_>> {
    if tail.len() < 5 || tail[0] != FMT_V1 {
        return None;
    }
    let schema_version = u32::from_be_bytes(tail[1..5].try_into().unwrap());
    let rest = &tail[5..];
    let (nlen, n) = get_varint(rest)?;
    let nlen = nlen as usize;
    let rest = rest.get(n..)?;
    let schema = std::str::from_utf8(rest.get(..nlen)?).ok()?;
    let rest = &rest[nlen..];
    let (tlen, n) = get_varint(rest)?;
    let tlen = tlen as usize;
    let rest = rest.get(n..)?;
    let type_name = std::str::from_utf8(rest.get(..tlen)?).ok()?;
    let msg = &rest[tlen..];
    Some(ProtoHead {
        schema_version,
        schema,
        type_name,
        msg,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let tail = encode("orders", 3, "shop.v1.Order", b"\x08\x2a");
        let h = decode(&tail).unwrap();
        assert_eq!(h.schema_version, 3);
        assert_eq!(h.schema, "orders");
        assert_eq!(h.type_name, "shop.v1.Order");
        assert_eq!(h.msg, b"\x08\x2a");
    }

    #[test]
    fn roundtrip_empty_message() {
        let tail = encode("s", 1, "pkg.Empty", b"");
        let h = decode(&tail).unwrap();
        assert_eq!(h.msg, b"");
        assert_eq!(h.type_name, "pkg.Empty");
    }

    #[test]
    fn long_names_roundtrip() {
        // Names past the 1-byte varint boundary.
        let schema = "s".repeat(300);
        let tname = format!("a.very.long.package.{}", "T".repeat(200));
        let msg = vec![0xABu8; 1000];
        let tail = encode(&schema, u32::MAX, &tname, &msg);
        let h = decode(&tail).unwrap();
        assert_eq!(h.schema_version, u32::MAX);
        assert_eq!(h.schema, schema);
        assert_eq!(h.type_name, tname);
        assert_eq!(h.msg, &msg[..]);
    }

    #[test]
    fn rejects_bad_fmt() {
        let mut tail = encode("s", 1, "T", b"x");
        tail[0] = 2;
        assert!(decode(&tail).is_none());
    }

    #[test]
    fn rejects_truncation() {
        let tail = encode("orders", 7, "shop.v1.Order", b"payload");
        // The msg tail may legitimately be empty, but every prefix that cuts
        // into the header or the names must fail cleanly.
        let msg_start = tail.len() - b"payload".len();
        for cut in 0..msg_start {
            assert!(decode(&tail[..cut]).is_none(), "cut at {cut} must fail");
        }
        assert!(decode(&tail[..msg_start]).is_some());
    }

    #[test]
    fn rejects_non_utf8_names() {
        let mut tail = Vec::new();
        tail.push(FMT_V1);
        tail.extend_from_slice(&1u32.to_be_bytes());
        tail.push(2); // nlen
        tail.extend_from_slice(&[0xFF, 0xFE]); // invalid utf-8 schema
        tail.push(1);
        tail.push(b'T');
        assert!(decode(&tail).is_none());
    }
}
