use marekvs_core::{
    envelope::{Envelope, RecordType},
    ikey,
};
use marekvs_engine::store::{self, expiry::ExpiryScheduler, ShardCtx, Store, StoreConfig};
use std::{sync::Arc, time::Duration};

fn open(dir: &tempfile::TempDir, shards: usize) -> Arc<Store> {
    Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 1,
        shard_threads: shards,
        sync_mode: ondadb::SyncMode::Interval,
    })
    .unwrap()
}
fn key(pid: u16, suffix: &[u8]) -> Vec<u8> {
    let mut k = ikey::string_key(suffix);
    k[..2].copy_from_slice(&pid.to_be_bytes());
    k
}
fn put(ctx: &ShardCtx, pid: u16, suffix: &[u8], deadline: u64) {
    store::put_raw(
        ctx,
        &key(pid, suffix),
        &Envelope::new(RecordType::String, ctx.hlc.now(), 1)
            .with_ttl(deadline)
            .encode_with(b"value"),
    );
}
fn discover(s: &mut ExpiryScheduler, ctx: &ShardCtx, now: u64) -> usize {
    let mut completed = 0;
    for _ in 0..20_000 {
        let progress = s.poll(ctx, now, 128, 8, Duration::from_secs(10)).unwrap();
        completed += progress.partitions_completed;
        if !progress.has_more_work {
            return completed;
        }
    }
    panic!("discovery did not converge");
}

#[tokio::test]
async fn no_ttl_discovery_parks_for_sixty_seconds_and_shards_are_disjoint() {
    for shards in [1, 2, 10] {
        let dir = tempfile::tempdir().unwrap();
        let store = open(&dir, shards);
        for shard in 0..shards {
            store
                .run(shard as u16, move |ctx| {
                    let now = store::now_ms();
                    put(ctx, shard as u16, b"no-ttl", 0);
                    let mut s =
                        ExpiryScheduler::new(ctx.maintenance.clone(), ctx.shard, ctx.shard_count);
                    let first = s.poll(ctx, now, 128, 8, Duration::from_secs(10)).unwrap();
                    assert_eq!(first.iterator_opens, 8);
                    assert_eq!(first.partitions_completed, 8);
                    let completed = 8 + discover(&mut s, ctx, now);
                    let count = (ikey::PARTITIONS as usize - 1 - shard) / shards + 1;
                    assert_eq!(completed, count);
                    for tick in 1..=600 {
                        let p = s
                            .poll(ctx, now + tick * 100, 128, 8, Duration::from_secs(10))
                            .unwrap();
                        assert_eq!(p.iterator_opens, 0);
                        assert_eq!(p.records_visited, 0);
                        assert!(!p.has_more_work);
                        assert_eq!(s.next_wait(now + tick * 100), Duration::from_secs(1));
                    }
                })
                .await;
        }
    }
}

#[tokio::test]
async fn future_ttl_persist_extension_and_clock_jumps() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    store
        .run(0, |ctx| {
            let now = store::now_ms();
            put(ctx, 0, b"due", now + 60_000);
            put(ctx, 0, b"persist", now + 60_000);
            put(ctx, 0, b"extend", now + 60_000);
            let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
            discover(&mut s, ctx, now);
            assert_eq!(
                s.poll(ctx, now - 60_000, 128, 8, Duration::from_secs(10))
                    .unwrap()
                    .iterator_opens,
                0
            );
            put(ctx, 0, b"persist", 0);
            put(ctx, 0, b"extend", now + 180_000);
            discover(&mut s, ctx, now);
            discover(&mut s, ctx, now + 60_001);
            assert!(
                Envelope::decode(&store::get_raw(ctx, &key(0, b"due")).unwrap())
                    .unwrap()
                    .0
                    .is_tombstone()
            );
            for k in [b"persist".as_slice(), b"extend"] {
                assert!(!Envelope::decode(&store::get_raw(ctx, &key(0, k)).unwrap())
                    .unwrap()
                    .0
                    .is_tombstone());
            }
            assert_eq!(ctx.maintenance.metrics.tombstones_written.get(), 1);
            discover(&mut s, ctx, now + 180_001);
            assert_eq!(ctx.maintenance.metrics.tombstones_written.get(), 2);
        })
        .await;
}

#[tokio::test]
async fn write_behind_cursor_invalidates_proof_and_busy_pid_rotates() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    store
        .run(0, |ctx| {
            let now = store::now_ms();
            for i in 0..80 {
                put(ctx, 0, format!("k{i:04}").as_bytes(), 0);
            }
            put(ctx, 1, b"future", now + 60_000);
            let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
            let first = s.poll(ctx, now, 128, 8, Duration::from_secs(10)).unwrap();
            assert_eq!(first.records_visited, 17);
            assert_eq!(first.partitions_completed, 7);
            put(ctx, 0, b"a-behind", now + 60_000);
            discover(&mut s, ctx, now);
            discover(&mut s, ctx, now + 60_001);
            assert_eq!(ctx.maintenance.metrics.tombstones_written.get(), 2);
        })
        .await;
}

#[tokio::test]
async fn observer_survives_replacement_and_suppression_and_range_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    store.set_commit_hook(Some(Arc::new(|_, _| {})));
    store.set_commit_hook(None);
    store
        .run(0, |ctx| {
            let before = ctx.maintenance.generation(0);
            {
                let _guard = store::suppress_commit_hook();
                put(ctx, 0, b"suppressed", 0);
            }
            assert!(ctx.maintenance.generation(0) > before);
            let before = ctx.maintenance.generation(0);
            store::delete_partition_range(ctx, 0).unwrap();
            assert!(ctx.maintenance.generation(0) > before);
        })
        .await;
}

#[tokio::test]
async fn tiny_elapsed_budget_still_advances_one_storage_record() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    store
        .run(0, |ctx| {
            put(ctx, 0, b"a", 0);
            put(ctx, 0, b"b", 0);
            let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
            let p = s
                .poll(ctx, store::now_ms(), 128, 8, Duration::from_nanos(1))
                .unwrap();
            assert_eq!(p.iterator_opens, 1);
            assert_eq!(p.records_visited, 1);
        })
        .await;
}

#[tokio::test]
async fn restart_rebuilds_unknown_state_and_replica_ttl_rearms_absence() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    let deadline = store::now_ms() + 60_000;
    store
        .run(0, move |ctx| {
            let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
            discover(&mut s, ctx, deadline - 60_000);
            let _origin = store::set_apply_origin(2);
            put(ctx, 0, b"replicated", deadline);
            discover(&mut s, ctx, deadline - 1);
            assert_eq!(s.next_wait(deadline - 1), Duration::from_millis(1));
        })
        .await;
    drop(store);
    let reopened = open(&dir, 1);
    reopened
        .run(0, move |ctx| {
            let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
            discover(&mut s, ctx, deadline + 1);
            assert!(
                Envelope::decode(&store::get_raw(ctx, &key(0, b"replicated")).unwrap())
                    .unwrap()
                    .0
                    .is_tombstone()
            );
        })
        .await;
}

#[tokio::test]
async fn busy_command_queue_does_not_starve_due_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    store
        .run(0, |ctx| put(ctx, 0, b"busy-due", store::now_ms() + 150))
        .await;
    // Jobs remain queued across several deadlines; no receive timeout occurs.
    let (tx, rx) = std::sync::mpsc::channel();
    for i in 0..300 {
        let tx = tx.clone();
        store.spawn_on(0, move |ctx| {
            std::thread::sleep(Duration::from_millis(2));
            if ctx.maintenance.metrics.tombstones_written.get() > 0 {
                let _ = tx.send(i);
            }
        });
    }
    let first = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("queued jobs starved expiry");
    assert!(first < 299, "expiry ran only after queue drained");
    store.run(0, |_| ()).await;
}

#[tokio::test]
async fn callback_barrier_serializes_proof_and_preserves_generation() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release = std::sync::Mutex::new(release_rx);
    store
        .maintenance
        .set_observer_barrier_for_tests(Some(Arc::new(move || {
            entered_tx.send(()).unwrap();
            release.lock().unwrap().recv().unwrap();
        })));
    store.spawn_on(0, |ctx| put(ctx, 0, b"paused", store::now_ms() + 60_000));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    // The commit is already visible to raw readers, but its writing shard is
    // still inside the callback. No proof publication can overtake it.
    assert!(store.db.get(&store.data, &key(0, b"paused")).is_ok());
    assert_eq!(store.partition_generation(0), 0);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    store.spawn_on(0, move |ctx| {
        let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
        discover(&mut s, ctx, store::now_ms());
        done_tx.send(s.next_wait(store::now_ms())).unwrap();
    });
    assert!(done_rx.try_recv().is_err());
    release_tx.send(()).unwrap();
    assert!(done_rx.recv_timeout(Duration::from_secs(3)).is_ok());
    store.maintenance.set_observer_barrier_for_tests(None);
}

#[tokio::test]
async fn member_ttl_keeps_remove_dots_and_budget_records_are_exempt() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    store
        .run(0, |ctx| {
            let now = store::now_ms();
            let mut member = ikey::set_member_key(b"set", b"member");
            member[..2].copy_from_slice(&0u16.to_be_bytes());
            let added = marekvs_core::merge::element_add_ttl(
                RecordType::SetMember,
                ctx.hlc.now(),
                1,
                b"member",
                now + 60_000,
            );
            store::put_raw(ctx, &member, &added);
            let mut budget = ikey::budget_slot_key(b"budget", 1, 1, 1);
            budget[..2].copy_from_slice(&0u16.to_be_bytes());
            store::put_raw(
                ctx,
                &budget,
                &Envelope::new(RecordType::String, ctx.hlc.now(), 1)
                    .with_ttl(now + 60_000)
                    .encode_with(b"budget-payload"),
            );
            let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
            discover(&mut s, ctx, now);
            discover(&mut s, ctx, now + 60_001);
            let removed = store::get_raw(ctx, &member).unwrap();
            let (env, payload) = Envelope::decode(&removed).unwrap();
            assert_eq!(env.hlc, (now + 60_000) << 16);
            assert!(marekvs_core::merge::element_value(payload).is_none());
            // Replaying the original live add cannot resurrect an expired
            // member: the remove retains its causal coverage.
            store::write_merged(ctx, &member, &added);
            let replayed = store::get_raw(ctx, &member).unwrap();
            assert!(
                marekvs_core::merge::element_value(Envelope::decode(&replayed).unwrap().1)
                    .is_none()
            );
            assert!(!Envelope::decode(&store::get_raw(ctx, &budget).unwrap())
                .unwrap()
                .0
                .is_tombstone());
            assert_eq!(ctx.maintenance.metrics.tombstones_written.get(), 1);
        })
        .await;
}

#[tokio::test]
async fn native_ttl_filtering_is_not_counted_as_active_tombstoning() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    store
        .run(0, |ctx| {
            let now = store::now_ms();
            let record = Envelope::new(RecordType::String, ctx.hlc.now(), 1)
                .with_ttl(now + 60_000)
                .encode_with(b"passively-removed");
            ctx.db
                .put(
                    &ctx.data,
                    &key(0, b"native"),
                    &record,
                    Duration::from_millis(1),
                )
                .unwrap();
            std::thread::sleep(Duration::from_millis(5));
            let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
            discover(&mut s, ctx, now + 60_001);
            assert_eq!(ctx.maintenance.metrics.tombstones_written.get(), 0);
            assert!(store::get_raw(ctx, &key(0, b"native")).is_none());
        })
        .await;
}

fn klog_files(dir: &std::path::Path, files: &mut Vec<std::path::PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            klog_files(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "klog") {
            files.push(path);
        }
    }
}

#[tokio::test]
async fn unreadable_sst_keeps_discovery_armed_and_cannot_prove_empty_purge() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    let path = dir.path().to_path_buf();
    store
        .run(0, move |ctx| {
            put(ctx, 0, b"on-disk", 0);
            ctx.db.flush_memtable(&ctx.data).unwrap();
            ctx.db.set_max_open_reader_bytes(1);
            let mut files = Vec::new();
            klog_files(&path, &mut files);
            assert!(!files.is_empty(), "fixture must have SST files");
            for file in &files {
                std::fs::rename(file, file.with_extension("hidden")).unwrap();
            }
            let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
            let error = s
                .poll(ctx, store::now_ms(), 128, 8, Duration::from_secs(10))
                .unwrap_err();
            assert_eq!(error.pid, 0);
            assert!(s.next_wait(store::now_ms()) <= Duration::from_millis(101));
            assert!(store::partition_has_data(ctx, 0).is_err());
            let ranges = ctx.data.stats().range_deletes;
            assert!(store::delete_partition_range(ctx, 0).is_err());
            assert_eq!(ctx.data.stats().range_deletes, ranges);
            for file in &files {
                std::fs::rename(file.with_extension("hidden"), file).unwrap();
            }
            std::thread::sleep(Duration::from_millis(110));
            discover(&mut s, ctx, store::now_ms());
            assert_eq!(
                s.poll(ctx, store::now_ms(), 128, 8, Duration::from_secs(10))
                    .unwrap()
                    .iterator_opens,
                0
            );
        })
        .await;
}

#[tokio::test]
async fn batched_multi_partition_and_derived_delete_ingress_invalidates_every_pid() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 2);
    store
        .run(0, |ctx| {
            let a = ctx.maintenance.generation(0);
            let b = ctx.maintenance.generation(2);
            let record = Envelope::new(RecordType::String, ctx.hlc.now(), 1).encode_with(b"batch");
            store::put_many_lww(
                ctx,
                &[(key(0, b"a"), record.clone()), (key(2, b"b"), record)],
            );
            assert!(ctx.maintenance.generation(0) > a);
            assert!(ctx.maintenance.generation(2) > b);
            let before = ctx.maintenance.generation(2);
            store::del_raw(ctx, &key(2, b"b"));
            assert!(ctx.maintenance.generation(2) > before);
            let before = ctx.maintenance.generation(0);
            let _guard = store::suppress_commit_hook();
            let mut txn = ctx.db.begin();
            txn.put(
                &ctx.data,
                &key(0, b"direct-transaction"),
                b"derived",
                Duration::ZERO,
            )
            .unwrap();
            txn.commit().unwrap();
            assert!(ctx.maintenance.generation(0) > before);
        })
        .await;
}

#[tokio::test]
async fn eight_continuously_dirty_parked_partitions_cannot_starve_unknown_discovery() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    store
        .run(0, |ctx| {
            let now = store::now_ms();
            let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
            assert_eq!(
                s.poll(ctx, now, 128, 8, Duration::from_secs(10))
                    .unwrap()
                    .partitions_completed,
                8
            );
            put(ctx, 8, b"behind-busy", now + 60_000);
            let initial_records = ctx.maintenance.metrics.records_visited.get();
            for _ in 0..8 {
                for pid in 0..8 {
                    s.invalidate(pid);
                }
                s.poll(ctx, now, 128, 8, Duration::from_secs(10)).unwrap();
            }
            assert!(
                ctx.maintenance.metrics.records_visited.get() > initial_records,
                "unknown pid 8 starved behind eight dirty parked pids"
            );
        })
        .await;
}

#[tokio::test]
async fn slow_expiry_commit_stops_at_elapsed_budget_and_retries_remaining_records() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(&dir, 1);
    store
        .run(0, |ctx| {
            let now = store::now_ms();
            for i in 0..16 {
                put(ctx, 0, format!("due-{i:02}").as_bytes(), now + 60_000);
            }
            let mut s = ExpiryScheduler::new(ctx.maintenance.clone(), 0, 1);
            discover(&mut s, ctx, now);
            ctx.maintenance
                .set_observer_barrier_for_tests(Some(Arc::new(|| {
                    std::thread::sleep(Duration::from_millis(20))
                })));
            let progress = s
                .poll(ctx, now + 60_001, 128, 8, Duration::from_millis(5))
                .unwrap();
            assert_eq!(
                progress.tombstones_written, 1,
                "one slow commit must not drain all observed expiries"
            );
            ctx.maintenance.set_observer_barrier_for_tests(None);
            discover(&mut s, ctx, now + 60_001);
            assert_eq!(ctx.maintenance.metrics.tombstones_written.get(), 16);
        })
        .await;
}
