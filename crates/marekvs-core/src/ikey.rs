//! Internal storage-key layouts (design/02).
//!
//! ```text
//! string          [pid:u16] [b's'] [userkey]
//! collection head [pid:u16] [b'M'] [varint klen] [userkey]
//! hash field      [pid:u16] [b'h'] [varint klen] [userkey] [field]
//! set member      [pid:u16] [b'S'] [varint klen] [userkey] [member]
//! zset member     [pid:u16] [b'z'] [varint klen] [userkey] [member]
//! zset score idx  [pid:u16] [b'Z'] [varint klen] [userkey] [score_be:u64] [member]
//! list element    [pid:u16] [b'q'] [varint klen] [userkey] [pos:u64]
//! list blob       [pid:u16] [b'l'] [userkey]   (RETIRED — see design/02 §Lists)
//! stream entry    [pid:u16] [b'x'] [varint klen] [userkey] [id_ms:u64] [id_seq:u64]
//! ```
//! All integers big-endian so keys memcmp-sort correctly.

use crate::pid_of;

pub type Pid = u16;
pub const PARTITIONS: u16 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tag {
    String = b's',
    Head = b'M',
    HashField = b'h',
    SetMember = b'S',
    ZsetMember = b'z',
    ZsetScore = b'Z',
    /// Per-element list record (position-keyed LWW register).
    ListElem = b'q',
    HllRegister = b'H',
    /// RETIRED whole-list LWW blob (pre-1.0). No longer written; the tag is
    /// kept reserved so old parse paths and RENAME plumbing stay total.
    List = b'l',
    StreamEntry = b'x',
    /// Budget records (design/13): escrow slots, tokens, admin sub-records.
    /// Element kind is the first suffix byte (`BUDGET_SLOT`/`BUDGET_TOKEN`/…).
    Budget = b'b',
    /// JSON document nodes (design/16): one record per JSON path. The suffix
    /// is a chain of self-delimiting segments (`crate::json::Seg`); the root
    /// record has an empty suffix. A node's key is a strict byte-prefix of
    /// every descendant's key, so subtree = prefix scan.
    Json = b'j',
    /// Protobuf field records (design/18): one record per field path of a
    /// decomposed proto value. The suffix is a chain of self-delimiting
    /// segments (`crate::pdoc::PSeg`, keyed by FIELD NUMBER); the root
    /// record has an empty suffix. Same parent-is-byte-prefix property as
    /// `Tag::Json`.
    ProtoField = b'p',
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

fn base(tag: Tag, userkey: &[u8], extra: usize) -> Vec<u8> {
    let mut k = Vec::with_capacity(3 + 2 + userkey.len() + extra);
    k.extend_from_slice(&pid_of(userkey).to_be_bytes());
    k.push(tag as u8);
    k
}

/// Simple keys: no element suffix, so no length prefix needed.
pub fn simple(tag: Tag, userkey: &[u8]) -> Vec<u8> {
    debug_assert!(matches!(tag, Tag::String | Tag::List));
    let mut k = base(tag, userkey, 0);
    k.extend_from_slice(userkey);
    k
}

/// Prefixed keys: `[varint klen][userkey]` then an element suffix.
pub fn prefixed(tag: Tag, userkey: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut k = base(tag, userkey, suffix.len() + 2);
    put_varint(&mut k, userkey.len() as u64);
    k.extend_from_slice(userkey);
    k.extend_from_slice(suffix);
    k
}

pub fn string_key(userkey: &[u8]) -> Vec<u8> {
    simple(Tag::String, userkey)
}
pub fn list_key(userkey: &[u8]) -> Vec<u8> {
    simple(Tag::List, userkey)
}

/// Origin position for a fresh list. Positions are unsigned so that memcmp
/// order over the big-endian suffix equals list order; the first element lands
/// at CENTER and pushes allocate outwards from it, giving ~2^63 headroom on
/// each side before a rebuild has to recenter.
pub const LIST_CENTER: u64 = 1u64 << 63;

/// Positions are allocated on a stride this wide, and the low
/// `log2(LIST_POS_STRIDE)` bits carry the **allocating node's id**.
///
/// Without the salt, two nodes pushing concurrently both read the same head
/// (or tail) and both allocate `head-1` — the same position. The element
/// records then merge LWW and one push is silently lost
/// (design/02 §Lists, "Concurrency caveat"). Salting makes that structurally
/// impossible: concurrent pushes from different nodes land in the *same slot*
/// but at different low bits, so both records survive and their relative order
/// is node-id order — arbitrary, but identical on every node, which is the
/// same guarantee Redis gives for concurrent pushes from different clients.
///
/// The full fix is a sequence CRDT (RGA); this is the cheap structural one.
/// Headroom: `2^63 / 1024 ≈ 9×10^15` pushes per direction before a recenter.
pub const LIST_POS_STRIDE: u64 = 1024;

/// Largest node id the position salt can represent (see [`LIST_POS_STRIDE`]).
pub const LIST_NODE_ID_MAX: u16 = (LIST_POS_STRIDE - 1) as u16;

/// Round a position down to its stride slot, discarding the node salt.
#[inline]
pub fn list_slot_base(pos: u64) -> u64 {
    pos & !(LIST_POS_STRIDE - 1)
}

/// Place `node` inside the slot starting at `slot_base`.
#[inline]
pub fn list_pos_in_slot(slot_base: u64, node: u16) -> u64 {
    list_slot_base(slot_base) | (node as u64 & (LIST_POS_STRIDE - 1))
}

/// Position for an LPUSH: the slot below `head`, salted for `node`.
///
/// Strictly less than `head` for every node id, because `head` lives at or
/// above its own slot base and this lands a whole slot below that.
#[inline]
pub fn list_pos_before(head: u64, node: u16) -> u64 {
    list_pos_in_slot(list_slot_base(head) - LIST_POS_STRIDE, node)
}

/// Position for an RPUSH: the slot above `tail`, salted for `node`.
#[inline]
pub fn list_pos_after(tail: u64, node: u16) -> u64 {
    list_pos_in_slot(list_slot_base(tail) + LIST_POS_STRIDE, node)
}

/// Position of the `index`-th element of a freshly rebuilt list.
#[inline]
pub fn list_pos_nth(index: u64, node: u16) -> u64 {
    list_pos_in_slot(LIST_CENTER + index * LIST_POS_STRIDE, node)
}

/// Element key for a list position: `[pid][b'q'][klen][userkey][pos u64 BE]`.
pub fn list_elem_key(userkey: &[u8], pos: u64) -> Vec<u8> {
    prefixed(Tag::ListElem, userkey, &pos.to_be_bytes())
}

/// Decode a list element position from a parsed key's suffix.
pub fn list_pos(suffix: &[u8]) -> Option<u64> {
    if suffix.len() >= 8 {
        Some(u64::from_be_bytes(suffix[..8].try_into().unwrap()))
    } else {
        None
    }
}
pub fn head_key(userkey: &[u8]) -> Vec<u8> {
    prefixed(Tag::Head, userkey, &[])
}
pub fn hash_field_key(userkey: &[u8], field: &[u8]) -> Vec<u8> {
    prefixed(Tag::HashField, userkey, field)
}
pub fn set_member_key(userkey: &[u8], member: &[u8]) -> Vec<u8> {
    prefixed(Tag::SetMember, userkey, member)
}
pub fn zset_member_key(userkey: &[u8], member: &[u8]) -> Vec<u8> {
    prefixed(Tag::ZsetMember, userkey, member)
}
pub fn zset_score_key(userkey: &[u8], score_be: u64, member: &[u8]) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(8 + member.len());
    suffix.extend_from_slice(&score_be.to_be_bytes());
    suffix.extend_from_slice(member);
    prefixed(Tag::ZsetScore, userkey, &suffix)
}
/// HyperLogLog register key: one record per touched register (design/02).
pub fn hll_register_key(userkey: &[u8], bucket: u16) -> Vec<u8> {
    prefixed(Tag::HllRegister, userkey, &bucket.to_be_bytes())
}

pub fn stream_entry_key(userkey: &[u8], id_ms: u64, id_seq: u64) -> Vec<u8> {
    let mut suffix = [0u8; 16];
    suffix[..8].copy_from_slice(&id_ms.to_be_bytes());
    suffix[8..].copy_from_slice(&id_seq.to_be_bytes());
    prefixed(Tag::StreamEntry, userkey, &suffix)
}

/// Scan prefix covering every element of one collection under `tag`.
pub fn collection_prefix(tag: Tag, userkey: &[u8]) -> Vec<u8> {
    prefixed(tag, userkey, &[])
}

/// Every element-bearing prefixed tag — what a whole-user-key transfer
/// (FetchCollection read-through, RENAME/COPY) must scan besides the
/// string/list/head records. ADD NEW ELEMENT TAGS HERE or cluster fetches
/// will silently miss the type's records.
pub const ELEMENT_TAGS: [Tag; 9] = [
    Tag::HashField,
    Tag::SetMember,
    Tag::ZsetMember,
    Tag::ListElem,
    Tag::StreamEntry,
    Tag::HllRegister,
    Tag::Budget,
    Tag::Json,
    Tag::ProtoField,
];

/// Budget element kinds — first byte of a `Tag::Budget` suffix. Memcmp order
/// groups all slots before all tokens within one budget's key range.
pub const BUDGET_SLOT: u8 = b'L';
pub const BUDGET_WINDOW_SLOT: u8 = b'W';
pub const BUDGET_TOKEN: u8 = b'T';

/// Pool-mode escrow slot: `[pid][b'b'][klen][userkey][b'L'][gen][node][epoch]`.
pub fn budget_slot_key(userkey: &[u8], gen: u64, node: u16, epoch: u64) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(1 + 8 + 2 + 8);
    suffix.push(BUDGET_SLOT);
    suffix.extend_from_slice(&gen.to_be_bytes());
    suffix.extend_from_slice(&node.to_be_bytes());
    suffix.extend_from_slice(&epoch.to_be_bytes());
    prefixed(Tag::Budget, userkey, &suffix)
}

/// Window-mode escrow slot:
/// `[pid][b'b'][klen][userkey][b'W'][gen][window][node][epoch]`.
pub fn budget_window_slot_key(
    userkey: &[u8],
    gen: u64,
    window: u64,
    node: u16,
    epoch: u64,
) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(1 + 8 + 8 + 2 + 8);
    suffix.push(BUDGET_WINDOW_SLOT);
    suffix.extend_from_slice(&gen.to_be_bytes());
    suffix.extend_from_slice(&window.to_be_bytes());
    suffix.extend_from_slice(&node.to_be_bytes());
    suffix.extend_from_slice(&epoch.to_be_bytes());
    prefixed(Tag::Budget, userkey, &suffix)
}

/// Token record: `[pid][b'b'][klen][userkey][b'T'][gen][hlc][node][epoch]` —
/// sorts by generation then issue time; `(hlc, node, epoch)` is cluster-unique
/// (HLC is per-process monotone; epoch disambiguates NodeId reuse).
pub fn budget_token_key(userkey: &[u8], gen: u64, hlc: u64, node: u16, epoch: u64) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(1 + 8 + 8 + 2 + 8);
    suffix.push(BUDGET_TOKEN);
    suffix.extend_from_slice(&gen.to_be_bytes());
    suffix.extend_from_slice(&hlc.to_be_bytes());
    suffix.extend_from_slice(&node.to_be_bytes());
    suffix.extend_from_slice(&epoch.to_be_bytes());
    prefixed(Tag::Budget, userkey, &suffix)
}

/// Scan prefix covering one budget's records of `kind` within `gen`
/// (kind = BUDGET_SLOT / BUDGET_WINDOW_SLOT / BUDGET_TOKEN).
pub fn budget_kind_prefix(userkey: &[u8], kind: u8, gen: u64) -> Vec<u8> {
    let mut suffix = Vec::with_capacity(1 + 8);
    suffix.push(kind);
    suffix.extend_from_slice(&gen.to_be_bytes());
    prefixed(Tag::Budget, userkey, &suffix)
}

/// JSON node key: `[pid][b'j'][klen][userkey][path-bytes]`. The root record
/// has an empty path; a node's key is the scan prefix of its subtree.
pub fn json_node_key(userkey: &[u8], path: &[u8]) -> Vec<u8> {
    prefixed(Tag::Json, userkey, path)
}

/// Proto field-record key: `[pid][b'p'][klen][userkey][path-bytes]`; the
/// root record has an empty path (design/18).
pub fn proto_field_key(userkey: &[u8], path: &[u8]) -> Vec<u8> {
    prefixed(Tag::ProtoField, userkey, path)
}

/// Scan prefix covering an entire partition (bootstrap, Merkle, purge).
pub fn partition_prefix(pid: Pid) -> Vec<u8> {
    pid.to_be_bytes().to_vec()
}

/// Parsed view of an internal key.
#[derive(Debug, PartialEq, Eq)]
pub struct ParsedKey<'a> {
    pub pid: Pid,
    pub tag: u8,
    pub userkey: &'a [u8],
    /// Element suffix (field/member/score+member/stream id); empty for
    /// simple + head keys.
    pub suffix: &'a [u8],
}

pub fn parse(ikey: &[u8]) -> Option<ParsedKey<'_>> {
    if ikey.len() < 3 {
        return None;
    }
    let pid = u16::from_be_bytes([ikey[0], ikey[1]]);
    let tag = ikey[2];
    let rest = &ikey[3..];
    match tag {
        b's' | b'l' => Some(ParsedKey {
            pid,
            tag,
            userkey: rest,
            suffix: &[],
        }),
        b'M' | b'h' | b'S' | b'z' | b'Z' | b'q' | b'x' | b'H' | b'b' | b'j' | b'p' => {
            let (klen, n) = get_varint(rest)?;
            let klen = klen as usize;
            if rest.len() < n + klen {
                return None;
            }
            Some(ParsedKey {
                pid,
                tag,
                userkey: &rest[n..n + klen],
                suffix: &rest[n + klen..],
            })
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- list position salting (design/02 §Lists, T2-13) ---

    /// The property the salt exists for: two nodes that observe the *same*
    /// head/tail — the concurrent-push race — never allocate the same
    /// position. Before salting they both produced `head-1` and one push was
    /// lost to LWW.
    #[test]
    fn concurrent_pushes_from_identical_state_never_collide() {
        let head = LIST_CENTER;
        let tail = LIST_CENTER;
        for a in 0..=LIST_NODE_ID_MAX {
            for b in (a + 1)..=LIST_NODE_ID_MAX {
                assert_ne!(
                    list_pos_before(head, a),
                    list_pos_before(head, b),
                    "LPUSH collision between nodes {a} and {b}"
                );
                assert_ne!(
                    list_pos_after(tail, a),
                    list_pos_after(tail, b),
                    "RPUSH collision between nodes {a} and {b}"
                );
            }
        }
    }

    /// Order between colliding-slot pushes must be node-id order, and it must
    /// be the same on every node (it is a pure function of the id).
    #[test]
    fn same_slot_order_is_node_id_order() {
        for a in 0..LIST_NODE_ID_MAX {
            let b = a + 1;
            assert!(list_pos_before(LIST_CENTER, a) < list_pos_before(LIST_CENTER, b));
            assert!(list_pos_after(LIST_CENTER, a) < list_pos_after(LIST_CENTER, b));
        }
    }

    /// A salted allocation must still land on the correct side of the list,
    /// for every node id — this is where an off-by-one in the masking shows.
    #[test]
    fn allocations_land_outside_the_observed_range() {
        let samples = [
            LIST_CENTER,
            LIST_CENTER + 1,
            LIST_CENTER + LIST_POS_STRIDE - 1,
            LIST_CENTER + LIST_POS_STRIDE,
            LIST_CENTER + 5 * LIST_POS_STRIDE + 7,
        ];
        for &pos in &samples {
            for node in [0u16, 1, 7, 511, LIST_NODE_ID_MAX] {
                assert!(
                    list_pos_before(pos, node) < pos,
                    "LPUSH at node {node} from {pos} did not sort before it"
                );
                assert!(
                    list_pos_after(pos, node) > pos,
                    "RPUSH at node {node} from {pos} did not sort after it"
                );
            }
        }
    }

    /// Rebuild positions ascend with the index and carry the salt, so a
    /// rebuild racing a remote push cannot collide with it either.
    #[test]
    fn rebuild_positions_ascend_and_carry_the_salt() {
        for node in [0u16, 3, LIST_NODE_ID_MAX] {
            let mut prev = list_pos_nth(0, node);
            assert_eq!(prev & (LIST_POS_STRIDE - 1), node as u64);
            for i in 1..64u64 {
                let p = list_pos_nth(i, node);
                assert!(p > prev, "rebuild position {i} did not ascend");
                assert_eq!(p & (LIST_POS_STRIDE - 1), node as u64);
                prev = p;
            }
        }
    }

    proptest::proptest! {
        /// Against a naive model: the slot an allocation lands in is exactly
        /// one below (or above) the observed position's slot, and the low bits
        /// are exactly the node id.
        #[test]
        fn allocator_matches_naive_model(
            pos in (LIST_POS_STRIDE * 2)..(u64::MAX - LIST_POS_STRIDE * 2),
            node in 0u16..=LIST_NODE_ID_MAX,
        ) {
            let slot = pos - (pos % LIST_POS_STRIDE);

            let lo = list_pos_before(pos, node);
            proptest::prop_assert_eq!(lo, slot - LIST_POS_STRIDE + node as u64);
            proptest::prop_assert!(lo < pos);

            let hi = list_pos_after(pos, node);
            proptest::prop_assert_eq!(hi, slot + LIST_POS_STRIDE + node as u64);
            proptest::prop_assert!(hi > pos);

            proptest::prop_assert_eq!(list_slot_base(lo), slot - LIST_POS_STRIDE);
            proptest::prop_assert_eq!(list_slot_base(hi), slot + LIST_POS_STRIDE);
        }

        /// Repeated pushes from one node stay strictly monotonic in both
        /// directions — the single-node list stays exactly ordered.
        #[test]
        fn repeated_single_node_pushes_are_monotonic(
            node in 0u16..=LIST_NODE_ID_MAX,
            steps in 1usize..64,
        ) {
            let mut head = list_pos_in_slot(LIST_CENTER, node);
            let mut tail = head;
            for _ in 0..steps {
                let nh = list_pos_before(head, node);
                proptest::prop_assert!(nh < head);
                head = nh;
                let nt = list_pos_after(tail, node);
                proptest::prop_assert!(nt > tail);
                tail = nt;
            }
        }
    }

    #[test]
    fn roundtrip_parse() {
        let k = hash_field_key(b"user:1", b"name");
        let p = parse(&k).unwrap();
        assert_eq!(p.userkey, b"user:1");
        assert_eq!(p.suffix, b"name");
        assert_eq!(p.tag, b'h');
        assert_eq!(p.pid, pid_of(b"user:1"));
    }

    #[test]
    fn collection_prefix_covers_elements() {
        let pfx = collection_prefix(Tag::SetMember, b"myset");
        let member = set_member_key(b"myset", b"a");
        assert!(member.starts_with(&pfx));
        // a longer user key must NOT fall under the shorter key's prefix
        let other = set_member_key(b"myset2", b"a");
        assert!(!other.starts_with(&pfx));
    }

    #[test]
    fn score_index_orders() {
        let low = zset_score_key(b"z", 100, b"a");
        let high = zset_score_key(b"z", 200, b"a");
        assert!(low < high);
    }

    #[test]
    fn list_elem_orders_by_position() {
        // Lower position sorts before higher (memcmp == list order).
        let lo = list_elem_key(b"q", LIST_CENTER - 1);
        let mid = list_elem_key(b"q", LIST_CENTER);
        let hi = list_elem_key(b"q", LIST_CENTER + 1);
        assert!(lo < mid && mid < hi);
        let p = parse(&mid).unwrap();
        assert_eq!(p.userkey, b"q");
        assert_eq!(p.tag, b'q');
        assert_eq!(list_pos(p.suffix), Some(LIST_CENTER));
        // A different user key must not fall under this key's prefix.
        let other = list_elem_key(b"q2", LIST_CENTER);
        let pfx = collection_prefix(Tag::ListElem, b"q");
        assert!(mid.starts_with(&pfx));
        assert!(!other.starts_with(&pfx));
    }

    #[test]
    fn simple_key_parse() {
        let k = string_key(b"hello");
        let p = parse(&k).unwrap();
        assert_eq!(p.userkey, b"hello");
        assert_eq!(p.tag, b's');
    }

    #[test]
    fn element_tags_all_parse_as_prefixed() {
        // ELEMENT_TAGS drives whole-key transfers (read-through fetch,
        // bootstrap): every entry must produce parseable prefixed keys.
        for tag in ELEMENT_TAGS {
            let k = prefixed(tag, b"k", b"suffix");
            let p = parse(&k).unwrap_or_else(|| panic!("{tag:?} must parse"));
            assert_eq!(p.tag, tag as u8);
            assert_eq!(p.userkey, b"k");
            assert_eq!(p.suffix, b"suffix");
        }
    }

    #[test]
    fn proto_field_roundtrip_parse() {
        let path = b"\x01\x01\x07"; // Field(7)
        let k = proto_field_key(b"user:1", path);
        let p = parse(&k).unwrap();
        assert_eq!(p.tag, b'p');
        assert_eq!(p.userkey, b"user:1");
        assert_eq!(p.suffix, path);
        // root = empty suffix = the whole-value scan prefix
        let root = proto_field_key(b"user:1", &[]);
        assert_eq!(root, collection_prefix(Tag::ProtoField, b"user:1"));
        assert!(k.starts_with(&root));
    }

    #[test]
    fn element_tags_include_proto_field() {
        assert!(
            ELEMENT_TAGS.contains(&Tag::ProtoField),
            "cluster fetches would silently miss proto field records"
        );
    }

    #[test]
    fn json_node_roundtrip_parse() {
        let path = b"\x01\x05title";
        let k = json_node_key(b"doc:1", path);
        let p = parse(&k).unwrap();
        assert_eq!(p.tag, b'j');
        assert_eq!(p.userkey, b"doc:1");
        assert_eq!(p.suffix, path);
        assert_eq!(p.pid, pid_of(b"doc:1"));
    }

    #[test]
    fn json_root_has_empty_suffix() {
        let k = json_node_key(b"doc:1", &[]);
        let p = parse(&k).unwrap();
        assert_eq!(p.suffix, b"");
        // root key covers the whole doc as a scan prefix
        let child = json_node_key(b"doc:1", b"\x01\x01a");
        assert!(child.starts_with(&k));
        assert_eq!(k, collection_prefix(Tag::Json, b"doc:1"));
    }

    #[test]
    fn json_parent_is_prefix_of_descendants_only() {
        let parent = json_node_key(b"d", b"\x01\x01a");
        let child = json_node_key(b"d", b"\x01\x01a\x01\x01b");
        let sibling = json_node_key(b"d", b"\x01\x02ab");
        assert!(child.starts_with(&parent));
        // "a" (len 1) vs "ab" (len 2): varint length byte diverges first
        assert!(!sibling.starts_with(&parent));
        // a longer user key must not fall under the shorter key's doc prefix
        let other_doc = json_node_key(b"d2", b"\x01\x01a");
        assert!(!other_doc.starts_with(&collection_prefix(Tag::Json, b"d")));
    }
}
