//! The ondaDB commit-hook contract, which design/10-testing.md §26-30 calls
//! "the canary for ondaDB upgrades".
//!
//! The replication ring is fed exclusively by ondaDB commit hooks
//! (design/01 §Replication ring), and cursor-resume replication assumes they
//! fire **exactly once per committed batch, with the full op list**
//! (design/00 §131, risky assumption 2). Nothing in marekvs can verify that at
//! runtime — `production-assessment.md:74` and `todo.md:213` both flag it as
//! load-bearing and unchecked — so it is pinned here instead.
//!
//! What is asserted is what the ring actually depends on:
//!
//! * every committed record reaches the hook exactly once (no drops, no
//!   duplicates) — a drop loses a write cluster-wide, a duplicate re-applies it;
//! * a multi-key transaction arrives as **one** invocation carrying **all** of
//!   its keys, never split or truncated;
//! * commit sequence numbers are unique, so a persisted high-water mark is
//!   unambiguous.
//!
//! The contract as written in design/00 also claims delivery happens "in commit
//! order". It does not, and never did — see
//! [`delivery_order_matches_commit_seq_order`] for the measurement, the cause in
//! ondaDB, and what it costs the ring.
//!
//! Deliberately **not** asserted: that the observed seq stream is *gap-free*.
//! Only the `data` column family carries a hook (`Store::set_commit_hook`),
//! while `meta` writes — the epoch, the ring high-water mark, applied-seq
//! cursors — consume sequence numbers silently. Gaps are therefore expected and
//! the ring must tolerate them.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use marekvs_core::ikey;
use marekvs_engine::store::{put_many_lww, put_raw, Store, StoreConfig};

/// One hook invocation: the batch's commit seq and the keys it carried.
type Batch = (u64, Vec<Vec<u8>>);

fn test_store(dir: &tempfile::TempDir, shard_threads: usize) -> Arc<Store> {
    Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 7,
        shard_threads,
        ..StoreConfig::default()
    })
    .unwrap()
}

/// Install a recording hook and hand back the shared log.
fn record(store: &Arc<Store>) -> Arc<Mutex<Vec<Batch>>> {
    let log: Arc<Mutex<Vec<Batch>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    store.set_commit_hook(Some(Arc::new(move |seq, ops| {
        sink.lock()
            .unwrap()
            .push((seq, ops.iter().map(|o| o.key.clone()).collect()));
    })));
    log
}

/// A distinct user key per (writer, index), so every write is identifiable.
fn key_for(writer: usize, i: usize) -> Vec<u8> {
    ikey::string_key(format!("w{writer}:k{i}").as_bytes())
}

#[tokio::test]
async fn every_committed_record_reaches_the_hook_exactly_once() {
    const WRITERS: usize = 8;
    const PER_WRITER: usize = 250;

    let dir = tempfile::tempdir().unwrap();
    let store = test_store(&dir, 4);
    let log = record(&store);

    // Concurrent committers spread across shards: keys hash to different pids,
    // so these land on different shard threads and commit in parallel.
    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..PER_WRITER {
                let k = key_for(w, i);
                store
                    .run_key(&k, {
                        let k = k.clone();
                        move |ctx| put_raw(ctx, &k, b"v")
                    })
                    .await;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let batches = log.lock().unwrap().clone();
    let expected: HashSet<Vec<u8>> = (0..WRITERS)
        .flat_map(|w| (0..PER_WRITER).map(move |i| key_for(w, i)))
        .collect();

    // Exactly once: count occurrences rather than collecting into a set, so a
    // duplicate delivery cannot hide behind deduplication.
    let mut seen: HashMap<Vec<u8>, usize> = HashMap::new();
    for (_, keys) in &batches {
        for k in keys {
            *seen.entry(k.clone()).or_default() += 1;
        }
    }

    let duplicated: Vec<_> = seen.iter().filter(|(_, n)| **n > 1).collect();
    assert!(
        duplicated.is_empty(),
        "commit hook fired more than once for {} key(s), e.g. {:?} — the ring \
         would re-apply these writes",
        duplicated.len(),
        duplicated.first()
    );

    let missing: Vec<_> = expected.iter().filter(|k| !seen.contains_key(*k)).collect();
    assert!(
        missing.is_empty(),
        "commit hook never fired for {} committed key(s), e.g. {:?} — these \
         writes would never replicate",
        missing.len(),
        missing.first()
    );

    // And nothing the test did not write.
    for k in seen.keys() {
        assert!(
            expected.contains(k),
            "commit hook reported a key that was never written: {k:?}"
        );
    }
}

#[tokio::test]
async fn a_batch_arrives_whole_and_in_one_invocation() {
    const BATCH: usize = 64;

    let dir = tempfile::tempdir().unwrap();
    let store = test_store(&dir, 2);
    let log = record(&store);

    // One transaction, many keys. They must share a pid so a single shard owns
    // the whole batch — that is what `put_many_lww` (MSET) does per shard.
    let anchor = b"batch-anchor".to_vec();
    let pid = marekvs_core::pid_of(&anchor);
    let items: Vec<(Vec<u8>, Vec<u8>)> = (0..BATCH)
        .map(|i| {
            (
                ikey::string_key(format!("batch:{i}").as_bytes()),
                b"v".to_vec(),
            )
        })
        .collect();
    let written: HashSet<Vec<u8>> = items.iter().map(|(k, _)| k.clone()).collect();

    store
        .run(pid, {
            let items = items.clone();
            move |ctx| put_many_lww(ctx, &items)
        })
        .await;

    let batches = log.lock().unwrap().clone();
    let carrying: Vec<&Batch> = batches
        .iter()
        .filter(|(_, keys)| keys.iter().any(|k| written.contains(k)))
        .collect();

    assert_eq!(
        carrying.len(),
        1,
        "a single committed transaction must produce exactly one hook \
         invocation, got {} — the ring would see a torn batch",
        carrying.len()
    );

    let delivered: HashSet<Vec<u8>> = carrying[0].1.iter().cloned().collect();
    assert_eq!(
        delivered,
        written,
        "the hook must carry the batch's FULL op list; {} of {BATCH} keys \
         were delivered",
        delivered.len()
    );
}

/// Commit seqs must be unique — this holds, and the ring depends on it.
///
/// The companion ordering property does **not** hold; see
/// [`delivery_order_matches_commit_seq_order`].
#[tokio::test]
async fn commit_seqs_are_unique() {
    const WRITERS: usize = 6;
    const PER_WRITER: usize = 150;

    let dir = tempfile::tempdir().unwrap();
    let store = test_store(&dir, 4);
    let log = record(&store);

    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..PER_WRITER {
                let k = key_for(w, i);
                store
                    .run_key(&k, {
                        let k = k.clone();
                        move |ctx| put_raw(ctx, &k, b"v")
                    })
                    .await;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let batches = log.lock().unwrap().clone();
    assert!(!batches.is_empty(), "no commits were observed");

    // Unique: two batches sharing a seq would make a persisted high-water mark
    // ambiguous, and cursor resume would skip or repeat one of them.
    let seqs: Vec<u64> = batches.iter().map(|(s, _)| *s).collect();
    let distinct: HashSet<u64> = seqs.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        seqs.len(),
        "commit seqs must be unique across batches: {} batches shared a seq",
        seqs.len() - distinct.len()
    );
}

/// **Known violation, pre-existing — not a regression from the 0.7.8 upgrade.**
///
/// `design/00-overview.md:131` states the contract as "fires exactly once per
/// committed batch, **in commit order**, with the full op list". The first and
/// third clauses hold (pinned by the tests above). The second does not:
/// concurrent committers deliver batches to the hook out of seq order — an
/// observed run saw seq 5 delivered before seq 2.
///
/// The cause is in ondaDB and is not new. `Txn::commit` assigns `commit_seq`
/// while holding the commit guard but calls `run_commit_hook` *after*
/// `drop(_guard)` (`src/txn.rs:544-549`), so two threads can be reordered
/// between taking their seq and reaching the hook. Verified identical at
/// v0.2.0 (`beeffd1`, same drop-then-hook shape), so marekvs has always had
/// this — it was simply never tested for.
///
/// Why it matters for the ring (`marekvs-repl/src/ring.rs`): `push` stores
/// entries in hook-delivery order but stamps them with the ondaDB seq, and
/// `read_after(after, max)` filters `e.seq > after` and then takes the first
/// `max`. It therefore assumes the buffer is sorted by seq. With a buffer of
/// `[seq9, seq3]` and a batch limit of one, the pump ships seq9, advances the
/// cursor to 9, and then filters seq3 out permanently — that op never
/// replicates through the ring. It is not lost data: anti-entropy repairs it on
/// the next round. So the effect is delayed convergence and some redundant
/// re-shipping, not divergence.
///
/// Left failing-but-ignored rather than silently dropped: fixing it means
/// either sorting on push or having `Ring::push` allocate its own monotonic
/// seq instead of trusting the hint, and both change replication behaviour that
/// the chaos/churn harness covers (the `crash_restart el-3600` cursor finding
/// in particular). That is a decision of its own, not a rider on an engine
/// upgrade.
#[tokio::test]
#[ignore = "known pre-existing violation: ondaDB runs the commit hook outside \
            the commit guard, so batches arrive out of seq order"]
async fn delivery_order_matches_commit_seq_order() {
    const WRITERS: usize = 6;
    const PER_WRITER: usize = 150;

    let dir = tempfile::tempdir().unwrap();
    let store = test_store(&dir, 4);
    let log = record(&store);

    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..PER_WRITER {
                let k = key_for(w, i);
                store
                    .run_key(&k, {
                        let k = k.clone();
                        move |ctx| put_raw(ctx, &k, b"v")
                    })
                    .await;
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let batches = log.lock().unwrap().clone();
    let mut prev = 0u64;
    for (i, (seq, _)) in batches.iter().enumerate() {
        assert!(
            *seq > prev,
            "commit seq went backwards at invocation {i}: {seq} after {prev} — \
             Ring::read_after would filter the lower-seq op out permanently"
        );
        prev = *seq;
    }
}
