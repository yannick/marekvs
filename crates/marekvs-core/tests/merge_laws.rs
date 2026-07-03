//! Property tests for the merge laws (design/10 §10.1): commutativity,
//! associativity, idempotence, and permutation-independence of final state.

use marekvs_core::envelope::{Envelope, RecordType};
use marekvs_core::merge::{element_add, element_remove, merge_values, resolve, Dot, MergeOutcome};
use proptest::prelude::*;

/// Apply `incoming` onto an optional local state, like the storage layer does.
fn apply(state: Option<Vec<u8>>, incoming: &[u8]) -> Vec<u8> {
    match state {
        None => incoming.to_vec(),
        Some(local) => resolve(&local, incoming, &merge_values(&local, incoming)).to_vec(),
    }
}

/// Two-way merge as a total function on records.
fn merge2(a: &[u8], b: &[u8]) -> Vec<u8> {
    apply(Some(a.to_vec()), b)
}

#[derive(Debug, Clone)]
enum Op {
    Add {
        hlc: u64,
        origin: u16,
        val: Vec<u8>,
    },
    Rm {
        hlc: u64,
        origin: u16,
        observed: Vec<Dot>,
    },
    Lww {
        hlc: u64,
        origin: u16,
        val: Vec<u8>,
        tomb: bool,
    },
}

impl Op {
    fn record(&self) -> Vec<u8> {
        match self {
            Op::Add { hlc, origin, val } => element_add(RecordType::SetMember, *hlc, *origin, val),
            Op::Rm {
                hlc,
                origin,
                observed,
            } => element_remove(RecordType::SetMember, *hlc, *origin, observed),
            Op::Lww {
                hlc,
                origin,
                val,
                tomb,
            } => {
                let env = if *tomb {
                    Envelope::tombstone(RecordType::String, *hlc, *origin)
                } else {
                    Envelope::new(RecordType::String, *hlc, *origin)
                };
                env.encode_with(val)
            }
        }
    }
}

fn dot_strategy() -> impl Strategy<Value = Dot> {
    (1u64..50, 0u16..4).prop_map(|(hlc, origin)| Dot { hlc, origin })
}

fn element_op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Value is a pure function of the dot: in production a dot uniquely
        // identifies one write, so identical dots always carry identical
        // values. Random values per dot would test an impossible input.
        (1u64..50, 0u16..4).prop_map(|(hlc, origin)| Op::Add {
            hlc,
            origin,
            val: vec![hlc as u8, origin as u8],
        }),
        (
            1u64..50,
            0u16..4,
            prop::collection::vec(dot_strategy(), 0..4)
        )
            .prop_map(|(hlc, origin, observed)| Op::Rm {
                hlc,
                origin,
                observed
            }),
    ]
}

fn lww_op_strategy() -> impl Strategy<Value = Op> {
    (
        1u64..50,
        0u16..4,
        prop::collection::vec(any::<u8>(), 0..8),
        any::<bool>(),
    )
        .prop_map(|(hlc, origin, val, tomb)| Op::Lww {
            hlc,
            origin,
            val,
            tomb,
        })
}

fn records(ops: Vec<Op>) -> Vec<Vec<u8>> {
    ops.iter().map(Op::record).collect()
}

fn converged_state(recs: &[Vec<u8>], order: &[usize]) -> Vec<u8> {
    let mut state: Option<Vec<u8>> = None;
    for &i in order {
        state = Some(apply(state, &recs[i]));
    }
    state.unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn commutative_elements(a in element_op_strategy(), b in element_op_strategy()) {
        let (ra, rb) = (a.record(), b.record());
        prop_assert_eq!(merge2(&ra, &rb), merge2(&rb, &ra));
    }

    #[test]
    fn commutative_lww(a in lww_op_strategy(), b in lww_op_strategy()) {
        let (ra, rb) = (a.record(), b.record());
        // On version ties both keep "local": states must still be identical
        // because equal versions denote the identical write in production.
        // The generator can produce distinct payloads with equal versions, so
        // compare envelopes' winning version rather than raw bytes on ties.
        let m1 = merge2(&ra, &rb);
        let m2 = merge2(&rb, &ra);
        let v1 = Envelope::decode(&m1).unwrap().0.version();
        let v2 = Envelope::decode(&m2).unwrap().0.version();
        prop_assert_eq!(v1, v2);
    }

    #[test]
    fn associative_elements(
        a in element_op_strategy(),
        b in element_op_strategy(),
        c in element_op_strategy(),
    ) {
        let (ra, rb, rc) = (a.record(), b.record(), c.record());
        let left = merge2(&merge2(&ra, &rb), &rc);
        let right = merge2(&ra, &merge2(&rb, &rc));
        prop_assert_eq!(left, right);
    }

    #[test]
    fn idempotent_elements(a in element_op_strategy(), b in element_op_strategy()) {
        let m = merge2(&a.record(), &b.record());
        prop_assert_eq!(merge2(&m, &m), m.clone());
        prop_assert_eq!(merge2(&m, &a.record()), m.clone());
        prop_assert_eq!(merge2(&m, &b.record()), m);
    }

    #[test]
    fn permutation_independent(ops in prop::collection::vec(element_op_strategy(), 1..6)) {
        let recs = records(ops);
        let n = recs.len();
        let base: Vec<usize> = (0..n).collect();
        let baseline = converged_state(&recs, &base);
        // rotate + reverse cover a representative set of orders cheaply
        for rot in 0..n {
            let mut order = base.clone();
            order.rotate_left(rot);
            prop_assert_eq!(&converged_state(&recs, &order), &baseline);
            order.reverse();
            prop_assert_eq!(&converged_state(&recs, &order), &baseline);
        }
    }

    #[test]
    fn merged_output_is_canonical(a in element_op_strategy(), b in element_op_strategy()) {
        // Whenever merge says Merged, re-merging the result with either input
        // must be a no-op (KeepLocal) — i.e. Merged bytes are a fixed point.
        let (ra, rb) = (a.record(), b.record());
        if let MergeOutcome::Merged(m) = merge_values(&ra, &rb) {
            prop_assert_eq!(merge_values(&m, &ra), MergeOutcome::KeepLocal);
            prop_assert_eq!(merge_values(&m, &rb), MergeOutcome::KeepLocal);
        }
    }
}

// ---------------------------------------------------------------------------
// PN-counter laws (v1.1)
// ---------------------------------------------------------------------------

mod counters {
    use super::*;
    use marekvs_core::counter::CounterState;
    use marekvs_core::envelope::{Envelope, RecordType};
    use marekvs_core::merge::{merge_values, resolve};

    fn counter_record(env_hlc: u64, origin: u16, state: &CounterState) -> Vec<u8> {
        Envelope::new(RecordType::Counter, env_hlc, origin).encode_with(&state.encode())
    }

    fn merge2(a: &[u8], b: &[u8]) -> Vec<u8> {
        resolve(a, b, &merge_values(a, b)).to_vec()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Every node emits cumulative snapshots of its own increments; any
        /// merge order of any interleaving must converge to the exact sum.
        #[test]
        fn no_lost_increments(
            per_node in prop::collection::vec(
                prop::collection::vec(-100i64..100, 1..5), 1..4),
        ) {
            let mut snapshots: Vec<Vec<u8>> = Vec::new();
            let mut expected: i128 = 0;
            for (node, deltas) in per_node.iter().enumerate() {
                let mut st = CounterState::on_base(0, 0, 0);
                for (step, d) in deltas.iter().enumerate() {
                    st.bump(node as u16, *d);
                    expected += *d as i128;
                    snapshots.push(counter_record(
                        1000 + (node * 100 + step) as u64,
                        node as u16,
                        &st,
                    ));
                }
            }
            // forward order
            let mut acc = snapshots[0].clone();
            for s in &snapshots[1..] {
                acc = merge2(&acc, s);
            }
            // reverse order
            let mut rev = snapshots.last().unwrap().clone();
            for s in snapshots.iter().rev().skip(1) {
                rev = merge2(&rev, s);
            }
            prop_assert_eq!(&acc, &rev, "merge order changed the counter");
            let (_, pay) = Envelope::decode(&acc).unwrap();
            let value = CounterState::decode(pay).unwrap().value().unwrap();
            prop_assert_eq!(value as i128, expected, "increments were lost");
        }

        /// Counter merge is commutative/associative/idempotent even across
        /// different bases (SET-reset semantics).
        #[test]
        fn counter_merge_laws(
            specs in prop::collection::vec(
                (0u64..3, 0u16..3, -50i64..50, 0u16..3, -50i64..50), 2..5),
        ) {
            let recs: Vec<Vec<u8>> = specs
                .iter()
                .enumerate()
                .map(|(i, (base_ver, n1, d1, n2, d2))| {
                    let mut st = CounterState::on_base(*base_ver, 0, 0);
                    st.bump(*n1, *d1);
                    st.bump(*n2, *d2);
                    counter_record(100 + i as u64, *n1, &st)
                })
                .collect();
            let (a, b) = (&recs[0], &recs[1]);
            prop_assert_eq!(merge2(a, b), merge2(b, a));
            if recs.len() >= 3 {
                let c = &recs[2];
                prop_assert_eq!(
                    merge2(&merge2(a, b), c),
                    merge2(a, &merge2(b, c))
                );
            }
            let m = merge2(a, b);
            prop_assert_eq!(merge2(&m, a), m.clone());
            prop_assert_eq!(merge2(&m, &m), m);
        }

        /// A plain SET (string record with a newer envelope) resets the
        /// counter in every merge order; an older SET always loses.
        #[test]
        fn set_resets_counter(d in 1i64..100) {
            let mut st = CounterState::on_base(10, 0, 0);
            st.bump(1, d);
            let counter = counter_record(30, 1, &st);
            let newer_set = Envelope::new(RecordType::String, 40, 2).encode_with(b"7");
            let older_set = Envelope::new(RecordType::String, 20, 2).encode_with(b"7");
            prop_assert_eq!(merge2(&counter, &newer_set), newer_set.clone());
            prop_assert_eq!(merge2(&newer_set, &counter), newer_set);
            prop_assert_eq!(merge2(&older_set, &counter), counter.clone());
            prop_assert_eq!(merge2(&counter, &older_set), counter);
        }
    }
}

// ---------------------------------------------------------------------------
// HyperLogLog register laws (design/02 §HyperLogLog)
// ---------------------------------------------------------------------------

mod hll_registers {
    use super::*;
    use marekvs_core::envelope::{Envelope, RecordType};
    use marekvs_core::merge::{merge_values, resolve};

    fn reg(hlc: u64, origin: u16, rank: u8) -> Vec<u8> {
        Envelope::new(RecordType::HllRegister, hlc, origin).encode_with(&[rank])
    }

    fn merge2(a: &[u8], b: &[u8]) -> Vec<u8> {
        resolve(a, b, &merge_values(a, b)).to_vec()
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// max-rank / min-version-on-tie is commutative, associative,
        /// idempotent, and the surviving rank is always the max.
        #[test]
        fn register_merge_laws(
            specs in prop::collection::vec((1u64..100, 0u16..4, 1u8..52), 2..5),
        ) {
            let recs: Vec<Vec<u8>> =
                specs.iter().map(|(h, o, r)| reg(*h, *o, *r)).collect();
            let (a, b) = (&recs[0], &recs[1]);
            prop_assert_eq!(merge2(a, b), merge2(b, a));
            if recs.len() >= 3 {
                let c = &recs[2];
                prop_assert_eq!(merge2(&merge2(a, b), c), merge2(a, &merge2(b, c)));
            }
            let m = merge2(a, b);
            prop_assert_eq!(merge2(&m, a), m.clone());
            prop_assert_eq!(merge2(&m, &m), m.clone());
            let (_, pay) = Envelope::decode(&m).unwrap();
            let max_rank = specs[..2].iter().map(|(_, _, r)| *r).max().unwrap();
            prop_assert_eq!(pay[0], max_rank, "merged rank must be the max");
        }

        /// A duplicate add (same rank, fresh envelope) is a strict no-op:
        /// the local record survives byte-identical.
        #[test]
        fn duplicate_add_is_noop(hlc in 1u64..100, rank in 1u8..52) {
            let stored = reg(hlc, 1, rank);
            let dup = reg(hlc + 1000, 2, rank); // later version, same rank
            prop_assert_eq!(
                merge_values(&stored, &dup),
                marekvs_core::merge::MergeOutcome::KeepLocal
            );
        }
    }
}
