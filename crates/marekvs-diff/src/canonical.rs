use crate::model::{Lid, Sid, Tree};
use xxhash_rust::xxh3::xxh3_128;
pub const VERSION_PREFIX: [u8; 6] = [0, 2, 0, 1, 0, 1];
pub(crate) fn field(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
}
pub fn serialize(t: &Tree) -> Vec<u8> {
    let mut out = VERSION_PREFIX.to_vec();
    field(&mut out, b"marekvs-diff/snapshot");
    let mut stack = vec![t.root];
    while let Some(id) = stack.pop() {
        let n = t.node(id);
        let path = t.ordinal_path(id);
        out.extend_from_slice(&(path.len() as u64).to_be_bytes());
        for o in path {
            out.extend_from_slice(&o.to_be_bytes());
        }
        out.push(n.kind as u8);
        field(&mut out, &serde_json::to_vec(&n.attrs).unwrap());
        field(&mut out, &serde_json::to_vec(&n.text).unwrap());
        out.extend_from_slice(&(n.children.len() as u64).to_be_bytes());
        stack.extend(n.children.iter().rev().copied());
    }
    out
}
pub fn sid(t: &Tree) -> Sid {
    Sid(xxh3_128(&serialize(t)))
}
pub fn assign_lids(t: &mut Tree) {
    for id in 0..t.nodes.len() {
        let mut b = b"marekvs-diff/lid".to_vec();
        b.extend_from_slice(&t.sid.0.to_be_bytes());
        let p = t.ordinal_path(id as u32);
        b.extend_from_slice(&(p.len() as u64).to_be_bytes());
        for o in p {
            b.extend_from_slice(&o.to_be_bytes());
        }
        t.nodes[id].lid = Lid(xxh3_128(&b));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn canonical_identity_ignores_eids_and_input_key_order() {
        let mut a =
            Tree::from_json(&json!({"t":"doc","a":{"z":1,"a":true},"c":[{"t":"sen","x":"ab"}]}))
                .unwrap();
        let b = Tree::from_json(
            &serde_json::from_str(r#"{"c":[{"x":"ab","t":"sen"}],"a":{"a":true,"z":1},"t":"doc"}"#)
                .unwrap(),
        )
        .unwrap();
        a.nodes[1].eid = Some([42; 10]);
        assert_eq!(serialize(&a), serialize(&b));
        assert_eq!(sid(&a), sid(&b));
        assert!(serialize(&a).starts_with(&VERSION_PREFIX));
    }
    #[test]
    fn canonical_distinguishes_text_boundaries_and_positions() {
        let a = Tree::from_json(&json!({"t":"doc","c":[{"t":"sen","x":"ab"},{"t":"sen","x":"c"}]}))
            .unwrap();
        let b = Tree::from_json(&json!({"t":"doc","c":[{"t":"sen","x":"a"},{"t":"sen","x":"bc"}]}))
            .unwrap();
        assert_ne!(a.sid, b.sid);
        assert_ne!(a.nodes[1].lid, a.nodes[2].lid);
    }
}
