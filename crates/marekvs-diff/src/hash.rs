use crate::{canonical::field, model::Tree};
use xxhash_rust::xxh3::xxh3_128;
/// Words are maximal non-whitespace runs; hard breaks are standalone tokens.
pub fn token_count(s: &str) -> usize {
    let mut count = 0;
    let mut word = false;
    for c in s.chars() {
        if c.is_whitespace() {
            word = false;
            if c == '\n' {
                count += 1;
            }
        } else {
            if !word {
                count += 1;
            }
            word = true;
        }
    }
    count
}
pub fn compute(t: &mut Tree) {
    for id in (0..t.nodes.len()).rev() {
        let n = &t.nodes[id];
        let mut content = b"marekvs-diff/content".to_vec();
        content.push(n.kind as u8);
        field(&mut content, &serde_json::to_vec(&n.attrs).unwrap());
        let mut exact = b"marekvs-diff/exact".to_vec();
        exact.push(n.kind as u8);
        field(&mut exact, &serde_json::to_vec(&n.attrs).unwrap());
        let weight = if let Some(text) = &n.text {
            field(&mut content, text.x.as_bytes());
            field(&mut exact, text.x.as_bytes());
            field(&mut exact, &serde_json::to_vec(&text.f).unwrap());
            token_count(&text.x) as u32
        } else {
            content.extend_from_slice(&(n.children.len() as u64).to_be_bytes());
            exact.extend_from_slice(&(n.children.len() as u64).to_be_bytes());
            let mut w = 0u32;
            for &child in &n.children {
                let c = t.node(child);
                content.extend_from_slice(&c.h_content.to_be_bytes());
                exact.extend_from_slice(&c.h_exact.to_be_bytes());
                w = w.saturating_add(c.weight);
            }
            w
        };
        let n = &mut t.nodes[id];
        n.weight = weight;
        n.h_content = xxh3_128(&content);
        n.h_exact = xxh3_128(&exact);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn hash_formats_and_structural_attrs() {
        let v = json!({"t":"doc","c":[{"t":"sen","x":"a b."}]});
        let a = Tree::from_json(&v).unwrap();
        let mut v = v;
        v["c"][0]["f"] = json!([[0,1,{"b":true}]]);
        let b = Tree::from_json(&v).unwrap();
        assert_eq!(a.node(0).h_content, b.node(0).h_content);
        assert_ne!(a.node(0).h_exact, b.node(0).h_exact);
        assert_eq!(a.node(0).weight, 2);
        v["c"][0]["a"] = json!({"role":"note"});
        let c = Tree::from_json(&v).unwrap();
        assert_ne!(b.node(0).h_content, c.node(0).h_content);
    }
    #[test]
    fn hash_token_count_keeps_hard_breaks() {
        assert_eq!(token_count("Héllo wörld\nnext"), 4);
        assert_eq!(token_count("  x\t\n "), 2);
    }
}
