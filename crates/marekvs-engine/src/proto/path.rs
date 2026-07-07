//! Dot-path field access over prost-reflect DynamicMessages (design/17).
//!
//! Path syntax: dot-separated segments, each a field NAME or field NUMBER;
//! a numeric segment after a repeated field is an index, after a map field
//! a key (parsed per the map's key type). Max 32 segments.
//!
//! Rendering (PROTO.GETFIELD): scalar leaves map to native RESP types,
//! enums to their value name, and message/repeated/map leaves to canonical
//! protobuf-JSON (64-bit ints as strings, bytes as base64, enums as names).

use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MapKey, ReflectMessage, Value};

use super::err::ProtoErr;
use crate::reply::Reply;

pub const MAX_SEGMENTS: usize = 32;

/// Parse a raw path argument into segments.
pub fn parse_path(raw: &[u8]) -> Result<Vec<String>, ProtoErr> {
    let s =
        std::str::from_utf8(raw).map_err(|_| ProtoErr::Path("path is not valid utf-8".into()))?;
    if s.is_empty() {
        return Err(ProtoErr::Path("empty path".into()));
    }
    let segs: Vec<String> = s.split('.').map(|p| p.to_string()).collect();
    if segs.len() > MAX_SEGMENTS {
        return Err(ProtoErr::Path(format!(
            "path exceeds {MAX_SEGMENTS} segments"
        )));
    }
    if segs.iter().any(|p| p.is_empty()) {
        return Err(ProtoErr::Path("empty path segment".into()));
    }
    Ok(segs)
}

/// Resolve one segment to a field of `msg`'s descriptor: by name, or — for
/// an all-digits segment — by field number.
pub(crate) fn resolve_field(
    msg: &prost_reflect::MessageDescriptor,
    seg: &str,
) -> Result<FieldDescriptor, ProtoErr> {
    if let Some(fd) = msg.get_field_by_name(seg) {
        return Ok(fd);
    }
    if seg.bytes().all(|b| b.is_ascii_digit()) {
        if let Some(fd) = seg.parse::<u32>().ok().and_then(|n| msg.get_field(n)) {
            return Ok(fd);
        }
    }
    Err(ProtoErr::Path(format!(
        "unknown field '{}' in {}",
        seg,
        msg.full_name()
    )))
}

pub(crate) fn parse_index(seg: &str) -> Result<usize, ProtoErr> {
    seg.parse::<usize>()
        .map_err(|_| ProtoErr::Path(format!("'{seg}' is not a list index")))
}

pub(crate) fn parse_map_key(seg: &str, key_kind: &Kind) -> Result<MapKey, ProtoErr> {
    let bad = || ProtoErr::Path(format!("map key '{seg}' does not parse as {key_kind:?}"));
    Ok(match key_kind {
        Kind::String => MapKey::String(seg.to_string()),
        Kind::Bool => MapKey::Bool(match seg {
            "true" | "1" => true,
            "false" | "0" => false,
            _ => return Err(bad()),
        }),
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => MapKey::I32(seg.parse().map_err(|_| bad())?),
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => MapKey::I64(seg.parse().map_err(|_| bad())?),
        Kind::Uint32 | Kind::Fixed32 => MapKey::U32(seg.parse().map_err(|_| bad())?),
        Kind::Uint64 | Kind::Fixed64 => MapKey::U64(seg.parse().map_err(|_| bad())?),
        other => {
            return Err(ProtoErr::Path(format!(
                "unsupported map key kind {other:?}"
            )))
        }
    })
}

// ---------------------------------------------------------------------------
// GET
// ---------------------------------------------------------------------------

/// A resolved leaf: the value plus the field context it was reached
/// through, and whether a list/map container was already indexed into
/// (`element` — the value is a single element, not the whole container).
pub struct Resolved {
    pub value: Value,
    pub field: FieldDescriptor,
    pub element: bool,
}

/// Walk `segs` from `root`. `Ok(None)` = unset/missing along the way
/// (renders as Null); `Err` = the path cannot exist for this type.
pub fn get_path(root: &DynamicMessage, segs: &[String]) -> Result<Option<Resolved>, ProtoErr> {
    let fd = resolve_field(&root.descriptor(), &segs[0])?;
    let rest = &segs[1..];
    if rest.is_empty() && fd.supports_presence() && !root.has_field(&fd) {
        return Ok(None);
    }
    if !rest.is_empty()
        && fd.supports_presence()
        && matches!(fd.kind(), Kind::Message(_))
        && !fd.is_map()
        && !root.has_field(&fd)
    {
        return Ok(None); // descending into an unset message
    }
    let value = root.get_field(&fd).into_owned();
    descend(value, fd, false, rest)
}

fn descend(
    value: Value,
    fd: FieldDescriptor,
    element: bool,
    segs: &[String],
) -> Result<Option<Resolved>, ProtoErr> {
    let Some(seg) = segs.first() else {
        return Ok(Some(Resolved {
            value,
            field: fd,
            element,
        }));
    };
    let rest = &segs[1..];
    if !element && fd.is_list() {
        let Value::List(items) = value else {
            return Err(ProtoErr::Path(
                "internal: repeated field is not a list".into(),
            ));
        };
        let idx = parse_index(seg)?;
        return match items.into_iter().nth(idx) {
            Some(v) => descend(v, fd, true, rest),
            None => Ok(None),
        };
    }
    if !element && fd.is_map() {
        let Value::Map(mut map) = value else {
            return Err(ProtoErr::Path("internal: map field is not a map".into()));
        };
        let Kind::Message(entry) = fd.kind() else {
            return Err(ProtoErr::Path("internal: map entry kind".into()));
        };
        let key = parse_map_key(seg, &entry.map_entry_key_field().kind())?;
        return match map.remove(&key) {
            Some(v) => descend(v, entry.map_entry_value_field(), false, rest),
            None => Ok(None),
        };
    }
    match value {
        Value::Message(m) => {
            let child = resolve_field(&m.descriptor(), seg)?;
            if rest.is_empty() && child.supports_presence() && !m.has_field(&child) {
                return Ok(None);
            }
            if !rest.is_empty()
                && child.supports_presence()
                && matches!(child.kind(), Kind::Message(_))
                && !child.is_map()
                && !m.has_field(&child)
            {
                return Ok(None);
            }
            let v = m.get_field(&child).into_owned();
            descend(v, child, false, rest)
        }
        _ => Err(ProtoErr::Path(format!(
            "cannot descend into scalar at '{seg}'"
        ))),
    }
}

/// Render a resolved leaf as a Reply (PROTO.GETFIELD semantics).
pub fn render(r: &Resolved) -> Reply {
    if !r.element && (r.field.is_list() || r.field.is_map()) {
        return json_bulk(value_to_json(&r.value, &r.field, r.element));
    }
    match &r.value {
        Value::Bool(b) => Reply::Bool(*b),
        Value::I32(v) => Reply::Int(*v as i64),
        Value::I64(v) => Reply::Int(*v),
        Value::U32(v) => Reply::Int(*v as i64),
        Value::U64(v) => {
            if *v <= i64::MAX as u64 {
                Reply::Int(*v as i64)
            } else {
                Reply::Bulk(v.to_string().into_bytes())
            }
        }
        Value::F32(v) => Reply::Double(*v as f64),
        Value::F64(v) => Reply::Double(*v),
        Value::String(s) => Reply::Bulk(s.clone().into_bytes()),
        Value::Bytes(b) => Reply::Bulk(b.to_vec()),
        Value::EnumNumber(n) => Reply::Bulk(enum_name(&r.field.kind(), *n).into_bytes()),
        Value::Message(_) | Value::List(_) | Value::Map(_) => {
            json_bulk(value_to_json(&r.value, &r.field, r.element))
        }
    }
}

fn json_bulk(v: serde_json::Value) -> Reply {
    Reply::Bulk(v.to_string().into_bytes())
}

fn enum_name(kind: &Kind, number: i32) -> String {
    if let Kind::Enum(ed) = kind {
        if let Some(v) = ed.get_value(number) {
            return v.name().to_string();
        }
    }
    number.to_string()
}

// ---------------------------------------------------------------------------
// canonical protobuf-JSON conversion for arbitrary Values
// ---------------------------------------------------------------------------

/// Canonical protobuf-JSON of a Value in a field context (`element` = the
/// value is one element of `field`'s list, not the container).
pub fn value_to_json(value: &Value, field: &FieldDescriptor, element: bool) -> serde_json::Value {
    if !element && field.is_map() {
        if let (Value::Map(map), Kind::Message(entry)) = (value, field.kind()) {
            let vfd = entry.map_entry_value_field();
            let mut obj = serde_json::Map::new();
            let mut entries: Vec<(String, &Value)> =
                map.iter().map(|(k, v)| (map_key_string(k), v)).collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            for (k, v) in entries {
                obj.insert(k, value_to_json(v, &vfd, false));
            }
            return serde_json::Value::Object(obj);
        }
        return serde_json::Value::Null;
    }
    if !element && field.is_list() {
        if let Value::List(items) = value {
            return serde_json::Value::Array(
                items
                    .iter()
                    .map(|v| value_to_json(v, field, true))
                    .collect(),
            );
        }
        return serde_json::Value::Null;
    }
    scalar_to_json(value, &field.kind())
}

fn scalar_to_json(value: &Value, kind: &Kind) -> serde_json::Value {
    use serde_json::Value as J;
    match value {
        Value::Bool(b) => J::Bool(*b),
        Value::I32(v) => J::Number((*v).into()),
        Value::U32(v) => J::Number((*v).into()),
        // Canonical protobuf-JSON: 64-bit ints render as strings.
        Value::I64(v) => J::String(v.to_string()),
        Value::U64(v) => J::String(v.to_string()),
        Value::F32(v) => float_json(*v as f64),
        Value::F64(v) => float_json(*v),
        Value::String(s) => J::String(s.clone()),
        Value::Bytes(b) => J::String(base64(b)),
        Value::EnumNumber(n) => J::String(enum_name(kind, *n)),
        Value::Message(m) => serde_json::to_value(m).unwrap_or(J::Null),
        // Nested containers only appear via map values (never list-in-list
        // in protobuf); handled by the callers with field context.
        Value::List(_) | Value::Map(_) => J::Null,
    }
}

fn float_json(f: f64) -> serde_json::Value {
    if f.is_nan() {
        serde_json::Value::String("NaN".into())
    } else if f == f64::INFINITY {
        serde_json::Value::String("Infinity".into())
    } else if f == f64::NEG_INFINITY {
        serde_json::Value::String("-Infinity".into())
    } else {
        serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    }
}

fn map_key_string(k: &MapKey) -> String {
    match k {
        MapKey::Bool(b) => b.to_string(),
        MapKey::I32(v) => v.to_string(),
        MapKey::I64(v) => v.to_string(),
        MapKey::U32(v) => v.to_string(),
        MapKey::U64(v) => v.to_string(),
        MapKey::String(s) => s.clone(),
    }
}

/// Standard base64 (RFC 4648, with padding) — protobuf-JSON bytes encoding.
/// Local helper to avoid a dependency for one call site.
pub fn base64(data: &[u8]) -> String {
    const AL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(AL[(n >> 18) as usize & 63] as char);
        out.push(AL[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            AL[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            AL[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

// ---------------------------------------------------------------------------
// value parsing (SETFIELD)
// ---------------------------------------------------------------------------

/// Parse a client-provided value for a field context: scalars from their
/// string form, message/repeated/map from JSON.
pub fn parse_value(raw: &[u8], field: &FieldDescriptor, element: bool) -> Result<Value, ProtoErr> {
    if !element && (field.is_list() || field.is_map()) {
        let json: serde_json::Value = serde_json::from_slice(raw).map_err(|e| {
            ProtoErr::Path(format!("value for '{}' is not JSON: {e}", field.name()))
        })?;
        return json_to_value(&json, field, false);
    }
    let kind = field.kind();
    let s = || std::str::from_utf8(raw).map_err(|_| bad_scalar(field, "utf-8"));
    let err = |what: &str| Err(bad_scalar(field, what));
    Ok(match kind {
        Kind::Bool => match s()? {
            "true" | "1" => Value::Bool(true),
            "false" | "0" => Value::Bool(false),
            _ => return err("bool"),
        },
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => match s()?.parse() {
            Ok(v) => Value::I32(v),
            Err(_) => return err("int32"),
        },
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => match s()?.parse() {
            Ok(v) => Value::I64(v),
            Err(_) => return err("int64"),
        },
        Kind::Uint32 | Kind::Fixed32 => match s()?.parse() {
            Ok(v) => Value::U32(v),
            Err(_) => return err("uint32"),
        },
        Kind::Uint64 | Kind::Fixed64 => match s()?.parse() {
            Ok(v) => Value::U64(v),
            Err(_) => return err("uint64"),
        },
        Kind::Float => match s()?.parse() {
            Ok(v) => Value::F32(v),
            Err(_) => return err("float"),
        },
        Kind::Double => match s()?.parse() {
            Ok(v) => Value::F64(v),
            Err(_) => return err("double"),
        },
        Kind::String => Value::String(s()?.to_string()),
        Kind::Bytes => Value::Bytes(raw.to_vec().into()),
        Kind::Enum(ed) => {
            let txt = s()?;
            if let Some(v) = ed.get_value_by_name(txt) {
                Value::EnumNumber(v.number())
            } else if let Ok(n) = txt.parse::<i32>() {
                Value::EnumNumber(n)
            } else {
                return Err(ProtoErr::Path(format!(
                    "'{}' is not a value of enum {}",
                    txt,
                    ed.full_name()
                )));
            }
        }
        Kind::Message(desc) => {
            let json: serde_json::Value = serde_json::from_slice(raw).map_err(|e| {
                ProtoErr::Path(format!("value for '{}' is not JSON: {e}", field.name()))
            })?;
            let m = DynamicMessage::deserialize(desc, json)
                .map_err(|e| ProtoErr::Path(format!("bad message value: {e}")))?;
            Value::Message(m)
        }
    })
}

fn bad_scalar(field: &FieldDescriptor, what: &str) -> ProtoErr {
    ProtoErr::Path(format!(
        "value for field '{}' does not parse as {what}",
        field.name()
    ))
}

/// JSON → Value for a field context (whole repeated/map fields and their
/// elements).
fn json_to_value(
    json: &serde_json::Value,
    field: &FieldDescriptor,
    element: bool,
) -> Result<Value, ProtoErr> {
    use serde_json::Value as J;
    if !element && field.is_map() {
        let J::Object(obj) = json else {
            return Err(ProtoErr::Path(format!(
                "map field '{}' takes a JSON object",
                field.name()
            )));
        };
        let Kind::Message(entry) = field.kind() else {
            return Err(ProtoErr::Path("internal: map entry kind".into()));
        };
        let kfd = entry.map_entry_key_field();
        let vfd = entry.map_entry_value_field();
        let mut map = std::collections::HashMap::new();
        for (k, v) in obj {
            map.insert(
                parse_map_key(k, &kfd.kind())?,
                json_to_value(v, &vfd, false)?,
            );
        }
        return Ok(Value::Map(map));
    }
    if !element && field.is_list() {
        let J::Array(items) = json else {
            return Err(ProtoErr::Path(format!(
                "repeated field '{}' takes a JSON array",
                field.name()
            )));
        };
        return Ok(Value::List(
            items
                .iter()
                .map(|v| json_to_value(v, field, true))
                .collect::<Result<_, _>>()?,
        ));
    }
    json_scalar_to_value(json, field)
}

fn json_scalar_to_value(
    json: &serde_json::Value,
    field: &FieldDescriptor,
) -> Result<Value, ProtoErr> {
    use serde_json::Value as J;
    let kind = field.kind();
    let err = |what: &str| {
        Err(ProtoErr::Path(format!(
            "JSON value for field '{}' does not parse as {what}",
            field.name()
        )))
    };
    // 64-bit ints accept both JSON numbers and canonical string form.
    let as_i64 = |j: &J| -> Option<i64> {
        match j {
            J::Number(n) => n.as_i64(),
            J::String(s) => s.parse().ok(),
            _ => None,
        }
    };
    let as_u64 = |j: &J| -> Option<u64> {
        match j {
            J::Number(n) => n.as_u64(),
            J::String(s) => s.parse().ok(),
            _ => None,
        }
    };
    Ok(match kind {
        Kind::Bool => match json {
            J::Bool(b) => Value::Bool(*b),
            _ => return err("bool"),
        },
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => match as_i64(json) {
            Some(v) if i32::try_from(v).is_ok() => Value::I32(v as i32),
            _ => return err("int32"),
        },
        Kind::Int64 | Kind::Sint64 | Kind::Sfixed64 => match as_i64(json) {
            Some(v) => Value::I64(v),
            _ => return err("int64"),
        },
        Kind::Uint32 | Kind::Fixed32 => match as_u64(json) {
            Some(v) if u32::try_from(v).is_ok() => Value::U32(v as u32),
            _ => return err("uint32"),
        },
        Kind::Uint64 | Kind::Fixed64 => match as_u64(json) {
            Some(v) => Value::U64(v),
            _ => return err("uint64"),
        },
        Kind::Float => match json.as_f64() {
            Some(v) => Value::F32(v as f32),
            None => return err("float"),
        },
        Kind::Double => match json.as_f64() {
            Some(v) => Value::F64(v),
            None => return err("double"),
        },
        Kind::String => match json {
            J::String(s) => Value::String(s.clone()),
            _ => return err("string"),
        },
        Kind::Bytes => match json {
            J::String(s) => match base64_decode(s) {
                Some(b) => Value::Bytes(b.into()),
                None => return err("base64 bytes"),
            },
            _ => return err("base64 bytes"),
        },
        Kind::Enum(ed) => match json {
            J::String(s) => match ed.get_value_by_name(s) {
                Some(v) => Value::EnumNumber(v.number()),
                None => return err("enum value"),
            },
            J::Number(n) => match n.as_i64() {
                Some(v) if i32::try_from(v).is_ok() => Value::EnumNumber(v as i32),
                _ => return err("enum value"),
            },
            _ => return err("enum value"),
        },
        Kind::Message(desc) => {
            let m = DynamicMessage::deserialize(desc, json.clone())
                .map_err(|e| ProtoErr::Path(format!("bad message value: {e}")))?;
            Value::Message(m)
        }
    })
}

/// Standard base64 decode (padding required), mirror of [`base64`].
pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => (c - b'A') as u32,
            b'a'..=b'z' => (c - b'a' + 26) as u32,
            b'0'..=b'9' => (c - b'0' + 52) as u32,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        })
    }
    let s = s.as_bytes();
    if s.len() % 4 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 4 * 3);
    for chunk in s.chunks(4) {
        let pad = chunk.iter().rev().take_while(|&&c| c == b'=').count();
        if pad > 2 {
            return None;
        }
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            let v = if c == b'=' {
                if i < chunk.len() - pad {
                    return None;
                }
                0
            } else {
                val(c)?
            };
            n = (n << 6) | v;
        }
        out.push((n >> 16) as u8);
        if pad < 2 {
            out.push((n >> 8) as u8);
        }
        if pad < 1 {
            out.push(n as u8);
        }
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// SET / CLEAR
// ---------------------------------------------------------------------------

/// Set `raw` at `segs` inside `root` (creates unset intermediate messages).
pub fn set_path(root: &mut DynamicMessage, segs: &[String], raw: &[u8]) -> Result<(), ProtoErr> {
    let fd = resolve_field(&root.descriptor(), &segs[0])?;
    if segs.len() == 1 {
        let v = parse_value(raw, &fd, false)?;
        root.set_field(&fd, v);
        return Ok(());
    }
    let val = root.get_field_mut(&fd); // inserts the default when unset
    set_in_value(val, &fd, false, &segs[1..], raw)
}

fn set_in_value(
    val: &mut Value,
    fd: &FieldDescriptor,
    element: bool,
    segs: &[String],
    raw: &[u8],
) -> Result<(), ProtoErr> {
    let seg = &segs[0];
    let rest = &segs[1..];
    if !element && fd.is_list() {
        let Value::List(items) = val else {
            return Err(ProtoErr::Path(
                "internal: repeated field is not a list".into(),
            ));
        };
        let idx = parse_index(seg)?;
        if idx < items.len() {
            if rest.is_empty() {
                items[idx] = parse_value(raw, fd, true)?;
                return Ok(());
            }
            return set_in_value(&mut items[idx], fd, true, rest, raw);
        }
        if idx == items.len() && rest.is_empty() {
            items.push(parse_value(raw, fd, true)?); // append at len
            return Ok(());
        }
        return Err(ProtoErr::Path(format!(
            "index {idx} out of range for '{}' (len {})",
            fd.name(),
            items.len()
        )));
    }
    if !element && fd.is_map() {
        let Value::Map(map) = val else {
            return Err(ProtoErr::Path("internal: map field is not a map".into()));
        };
        let Kind::Message(entry) = fd.kind() else {
            return Err(ProtoErr::Path("internal: map entry kind".into()));
        };
        let key = parse_map_key(seg, &entry.map_entry_key_field().kind())?;
        let vfd = entry.map_entry_value_field();
        if rest.is_empty() {
            map.insert(key, parse_value(raw, &vfd, false)?);
            return Ok(());
        }
        let slot = map
            .entry(key)
            .or_insert_with(|| Value::default_value_for_field(&vfd));
        return set_in_value(slot, &vfd, false, rest, raw);
    }
    match val {
        Value::Message(m) => {
            let child = resolve_field(&m.descriptor(), seg)?;
            if rest.is_empty() {
                let v = parse_value(raw, &child, false)?;
                m.set_field(&child, v);
                return Ok(());
            }
            let next = m.get_field_mut(&child);
            set_in_value(next, &child, false, rest, raw)
        }
        _ => Err(ProtoErr::Path(format!(
            "cannot descend into scalar at '{seg}'"
        ))),
    }
}

/// Clear the target of `segs` (field reset / list-element removal / map-key
/// removal). Returns whether something was present to clear.
pub fn clear_path(root: &mut DynamicMessage, segs: &[String]) -> Result<bool, ProtoErr> {
    let fd = resolve_field(&root.descriptor(), &segs[0])?;
    if segs.len() == 1 {
        let had = root.has_field(&fd);
        root.clear_field(&fd);
        return Ok(had);
    }
    if !root.has_field(&fd) {
        return Ok(false);
    }
    let val = root.get_field_mut(&fd);
    clear_in_value(val, &fd, false, &segs[1..])
}

fn clear_in_value(
    val: &mut Value,
    fd: &FieldDescriptor,
    element: bool,
    segs: &[String],
) -> Result<bool, ProtoErr> {
    let seg = &segs[0];
    let rest = &segs[1..];
    if !element && fd.is_list() {
        let Value::List(items) = val else {
            return Err(ProtoErr::Path(
                "internal: repeated field is not a list".into(),
            ));
        };
        let idx = parse_index(seg)?;
        if idx >= items.len() {
            return Ok(false);
        }
        if rest.is_empty() {
            items.remove(idx);
            return Ok(true);
        }
        return clear_in_value(&mut items[idx], fd, true, rest);
    }
    if !element && fd.is_map() {
        let Value::Map(map) = val else {
            return Err(ProtoErr::Path("internal: map field is not a map".into()));
        };
        let Kind::Message(entry) = fd.kind() else {
            return Err(ProtoErr::Path("internal: map entry kind".into()));
        };
        let key = parse_map_key(seg, &entry.map_entry_key_field().kind())?;
        if rest.is_empty() {
            return Ok(map.remove(&key).is_some());
        }
        let Some(slot) = map.get_mut(&key) else {
            return Ok(false);
        };
        return clear_in_value(slot, &entry.map_entry_value_field(), false, rest);
    }
    match val {
        Value::Message(m) => {
            let child = resolve_field(&m.descriptor(), seg)?;
            if rest.is_empty() {
                let had = m.has_field(&child);
                m.clear_field(&child);
                return Ok(had);
            }
            if !m.has_field(&child) {
                return Ok(false);
            }
            clear_in_value(m.get_field_mut(&child), &child, false, rest)
        }
        _ => Err(ProtoErr::Path(format!(
            "cannot descend into scalar at '{seg}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::compile::{compile_source, pool_from_fds};
    use crate::proto::ProtoLimits;
    use prost::Message;
    use prost_reflect::DescriptorPool;

    fn pool() -> DescriptorPool {
        let src = r#"
            syntax = "proto3";
            package t;
            enum Color { COLOR_UNSPECIFIED = 0; RED = 1; BLUE = 2; }
            message Inner { string note = 1; uint64 big = 2; }
            message Outer {
                string name = 1;
                int32 count = 2;
                double ratio = 3;
                bool ok = 4;
                bytes blob = 5;
                Color color = 6;
                Inner inner = 7;
                repeated Inner items = 8;
                map<string, int64> scores = 9;
                repeated string tags = 10;
                map<int32, Inner> by_id = 11;
            }
        "#;
        let limits = ProtoLimits::from_env();
        let out = compile_source("t", src, Default::default(), &limits).unwrap();
        pool_from_fds(&out.fds).unwrap()
    }

    fn outer(pool: &DescriptorPool) -> DynamicMessage {
        DynamicMessage::new(pool.get_message_by_name("t.Outer").unwrap())
    }

    fn seg(path: &str) -> Vec<String> {
        parse_path(path.as_bytes()).unwrap()
    }

    fn set(m: &mut DynamicMessage, path: &str, v: &[u8]) {
        set_path(m, &seg(path), v).unwrap();
    }

    fn get(m: &DynamicMessage, path: &str) -> Reply {
        match get_path(m, &seg(path)).unwrap() {
            Some(r) => render(&r),
            None => Reply::Null,
        }
    }

    #[test]
    fn parse_path_limits() {
        assert!(parse_path(b"").is_err());
        assert!(parse_path(b"a..b").is_err());
        let depth33 = vec!["a"; 33].join(".");
        assert!(matches!(
            parse_path(depth33.as_bytes()),
            Err(ProtoErr::Path(_))
        ));
        let depth32 = vec!["a"; 32].join(".");
        assert_eq!(parse_path(depth32.as_bytes()).unwrap().len(), 32);
    }

    #[test]
    fn scalar_set_get_roundtrip() {
        let p = pool();
        let mut m = outer(&p);
        set(&mut m, "name", b"alice");
        set(&mut m, "count", b"-3");
        set(&mut m, "ratio", b"2.5");
        set(&mut m, "ok", b"true");
        assert_eq!(get(&m, "name"), Reply::Bulk(b"alice".to_vec()));
        assert_eq!(get(&m, "count"), Reply::Int(-3));
        assert_eq!(get(&m, "ratio"), Reply::Double(2.5));
        assert_eq!(get(&m, "ok"), Reply::Bool(true));
    }

    #[test]
    fn field_number_segments_resolve() {
        let p = pool();
        let mut m = outer(&p);
        set(&mut m, "1", b"bob"); // field 1 = name
        assert_eq!(get(&m, "name"), Reply::Bulk(b"bob".to_vec()));
        assert_eq!(get(&m, "1"), Reply::Bulk(b"bob".to_vec()));
    }

    #[test]
    fn enum_renders_name_and_parses_both_forms() {
        let p = pool();
        let mut m = outer(&p);
        set(&mut m, "color", b"BLUE");
        assert_eq!(get(&m, "color"), Reply::Bulk(b"BLUE".to_vec()));
        set(&mut m, "color", b"1");
        assert_eq!(get(&m, "color"), Reply::Bulk(b"RED".to_vec()));
    }

    #[test]
    fn unset_message_field_is_null() {
        let p = pool();
        let m = outer(&p);
        assert_eq!(get(&m, "inner"), Reply::Null);
        assert_eq!(get(&m, "inner.note"), Reply::Null);
        // proto3 scalar without presence: default, not Null
        assert_eq!(get(&m, "count"), Reply::Int(0));
    }

    #[test]
    fn nested_set_creates_intermediates() {
        let p = pool();
        let mut m = outer(&p);
        set(&mut m, "inner.note", b"hi");
        set(&mut m, "inner.big", b"18446744073709551615");
        assert_eq!(get(&m, "inner.note"), Reply::Bulk(b"hi".to_vec()));
        // u64 above i64::MAX renders as its decimal string
        assert_eq!(
            get(&m, "inner.big"),
            Reply::Bulk(b"18446744073709551615".to_vec())
        );
    }

    #[test]
    fn repeated_index_and_whole_list_json() {
        let p = pool();
        let mut m = outer(&p);
        set(&mut m, "tags", br#"["a","b"]"#);
        assert_eq!(get(&m, "tags.0"), Reply::Bulk(b"a".to_vec()));
        assert_eq!(get(&m, "tags.1"), Reply::Bulk(b"b".to_vec()));
        assert_eq!(get(&m, "tags.2"), Reply::Null);
        assert_eq!(get(&m, "tags"), Reply::Bulk(br#"["a","b"]"#.to_vec()));
        // element replace + append-at-len
        set(&mut m, "tags.0", b"z");
        set(&mut m, "tags.2", b"c");
        assert_eq!(get(&m, "tags"), Reply::Bulk(br#"["z","b","c"]"#.to_vec()));
        // out-of-range set errors
        assert!(matches!(
            set_path(&mut m, &seg("tags.9"), b"x"),
            Err(ProtoErr::Path(_))
        ));
    }

    #[test]
    fn repeated_message_paths() {
        let p = pool();
        let mut m = outer(&p);
        set(
            &mut m,
            "items",
            br#"[{"note":"n0"},{"note":"n1","big":"7"}]"#,
        );
        assert_eq!(get(&m, "items.1.note"), Reply::Bulk(b"n1".to_vec()));
        assert_eq!(get(&m, "items.1.big"), Reply::Int(7));
        set(&mut m, "items.0.note", b"patched");
        assert_eq!(get(&m, "items.0.note"), Reply::Bulk(b"patched".to_vec()));
    }

    #[test]
    fn map_paths() {
        let p = pool();
        let mut m = outer(&p);
        set(&mut m, "scores", br#"{"alice":"10","bob":20}"#);
        assert_eq!(get(&m, "scores.alice"), Reply::Int(10));
        assert_eq!(get(&m, "scores.bob"), Reply::Int(20));
        assert_eq!(get(&m, "scores.carol"), Reply::Null);
        // whole map renders canonical (i64 values as strings, sorted keys)
        assert_eq!(
            get(&m, "scores"),
            Reply::Bulk(br#"{"alice":"10","bob":"20"}"#.to_vec())
        );
        // int-keyed map with message values, deep set through missing key
        set(&mut m, "by_id.5.note", b"five");
        assert_eq!(get(&m, "by_id.5.note"), Reply::Bulk(b"five".to_vec()));
        set(&mut m, "scores.dave", b"30");
        assert_eq!(get(&m, "scores.dave"), Reply::Int(30));
    }

    #[test]
    fn unknown_field_is_protopath() {
        let p = pool();
        let m = outer(&p);
        assert!(matches!(
            get_path(&m, &seg("nosuch")),
            Err(ProtoErr::Path(_))
        ));
        let mut m2 = outer(&p);
        assert!(matches!(
            set_path(&mut m2, &seg("nosuch"), b"1"),
            Err(ProtoErr::Path(_))
        ));
    }

    #[test]
    fn scalar_descent_is_protopath() {
        let p = pool();
        let mut m = outer(&p);
        set(&mut m, "name", b"x");
        assert!(matches!(
            get_path(&m, &seg("name.deeper")),
            Err(ProtoErr::Path(_))
        ));
    }

    #[test]
    fn clear_field_list_and_map() {
        let p = pool();
        let mut m = outer(&p);
        set(&mut m, "name", b"x");
        set(&mut m, "tags", br#"["a","b","c"]"#);
        set(&mut m, "scores", br#"{"a":"1","b":"2"}"#);
        set(&mut m, "inner.note", b"deep");

        assert!(clear_path(&mut m, &seg("tags.1")).unwrap());
        assert_eq!(get(&m, "tags"), Reply::Bulk(br#"["a","c"]"#.to_vec()));
        assert!(clear_path(&mut m, &seg("scores.a")).unwrap());
        assert_eq!(get(&m, "scores.a"), Reply::Null);
        assert!(!clear_path(&mut m, &seg("scores.zzz")).unwrap());
        assert!(clear_path(&mut m, &seg("inner.note")).unwrap());
        // proto3 scalar without presence: cleared = default, not Null
        assert_eq!(get(&m, "inner.note"), Reply::Bulk(Vec::new()));
        assert!(clear_path(&mut m, &seg("inner")).unwrap());
        assert_eq!(get(&m, "inner"), Reply::Null);
    }

    #[test]
    fn set_preserves_untouched_fields_through_reencode() {
        let p = pool();
        let mut m = outer(&p);
        set(&mut m, "name", b"keep");
        set(&mut m, "count", b"42");
        set(&mut m, "inner.note", b"deep");
        let bytes = m.encode_to_vec();
        // decode → set one field → the others survive
        let desc = p.get_message_by_name("t.Outer").unwrap();
        let mut m2 = DynamicMessage::decode(desc, &bytes[..]).unwrap();
        set(&mut m2, "ratio", b"9.5");
        assert_eq!(get(&m2, "name"), Reply::Bulk(b"keep".to_vec()));
        assert_eq!(get(&m2, "count"), Reply::Int(42));
        assert_eq!(get(&m2, "inner.note"), Reply::Bulk(b"deep".to_vec()));
        assert_eq!(get(&m2, "ratio"), Reply::Double(9.5));
    }

    #[test]
    fn bytes_field_base64_json_roundtrip() {
        let p = pool();
        let mut m = outer(&p);
        set(&mut m, "blob", &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(get(&m, "blob"), Reply::Bulk(vec![0xDE, 0xAD, 0xBE, 0xEF]));
        assert_eq!(base64(&[0xDE, 0xAD, 0xBE, 0xEF]), "3q2+7w==");
        assert_eq!(
            base64_decode("3q2+7w==").unwrap(),
            vec![0xDE, 0xAD, 0xBE, 0xEF]
        );
        assert_eq!(base64(b"a"), "YQ==");
        assert_eq!(base64_decode("YQ==").unwrap(), b"a".to_vec());
        assert_eq!(base64(b""), "");
        assert!(base64_decode("bad!").is_none());
    }
}
