//! BG.* engine integration tests (design/13): command matrix plus the
//! fix-targeted cases from the adversarial review — generation fencing,
//! replay/tear convergence, checked arithmetic, reqid dedup, restart.

use std::sync::Arc;

use marekvs_engine::cmd::{budget, generic, string as string_cmd};
use marekvs_engine::reply::Reply;
use marekvs_engine::store::{Store, StoreConfig};
use marekvs_engine::Engine;

fn engine() -> (tempfile::TempDir, Arc<Engine>) {
    let dir = tempfile::tempdir().unwrap();
    let e = open(&dir, 7);
    (dir, e)
}

fn open(dir: &tempfile::TempDir, node_id: u16) -> Arc<Engine> {
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id,
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap();
    Engine::new(store)
}

fn a(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}

fn int(r: Reply) -> i64 {
    match r {
        Reply::Int(n) => n,
        other => panic!("expected Int, got {other:?}"),
    }
}

fn ok(r: Reply) {
    assert_eq!(r, Reply::Simple("OK"));
}

fn assert_err_contains(r: Reply, needle: &str) {
    match r {
        Reply::Err(e) => assert!(e.contains(needle), "error {e:?} did not contain {needle:?}"),
        other => panic!("expected Err containing {needle:?}, got {other:?}"),
    }
}

fn map_get(r: &Reply, key: &str) -> Reply {
    match r {
        Reply::Map(pairs) => pairs
            .iter()
            .find_map(|(k, v)| match k {
                Reply::Bulk(k) if k == key.as_bytes() => Some(v.clone()),
                _ => None,
            })
            .unwrap_or_else(|| panic!("missing map field {key}")),
        other => panic!("expected Map, got {other:?}"),
    }
}

fn token_of(r: &Reply) -> Vec<u8> {
    match map_get(r, "token") {
        Reply::Bulk(t) => t,
        other => panic!("expected token bulk, got {other:?}"),
    }
}

async fn reserve(e: &Arc<Engine>, key: &[u8], amount: &str, extra: &[&[u8]]) -> Reply {
    let mut args = a(&[b"BG.RESERVE", key, amount.as_bytes()]);
    args.extend(extra.iter().map(|p| p.to_vec()));
    budget::reserve(e, &args).await
}

async fn info_field(e: &Arc<Engine>, key: &[u8], field: &str) -> i64 {
    let r = budget::info(e, &a(&[b"BG.INFO", key])).await;
    int(map_get(&r, field))
}

#[tokio::test]
async fn create_reserve_commit_lifecycle() {
    let (_d, e) = engine();
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    assert_eq!(info_field(&e, b"b", "capacity").await, 100);
    assert_eq!(info_field(&e, b"b", "available").await, 100);

    let g = reserve(&e, b"b", "40", &[]).await;
    assert_eq!(int(map_get(&g, "amount")), 40);
    assert!(int(map_get(&g, "deadline")) > 0);
    assert_eq!(info_field(&e, b"b", "available").await, 60);
    assert_eq!(info_field(&e, b"b", "open-tokens").await, 1);

    let tok = token_of(&g);
    // Spend 25 of the 40 → 15 flows back; 25 is permanently consumed.
    let credited = int(budget::commit(&e, &a(&[b"BG.COMMIT", b"b", &tok, b"25"])).await);
    assert_eq!(credited, 15);
    assert_eq!(info_field(&e, b"b", "available").await, 75);
    assert_eq!(info_field(&e, b"b", "open-tokens").await, 0);

    // Double COMMIT / RELEASE after fold → TOKENUSED.
    assert_err_contains(
        budget::commit(&e, &a(&[b"BG.COMMIT", b"b", &tok, b"25"])).await,
        "TOKENUSED",
    );
    assert_err_contains(
        budget::release(&e, &a(&[b"BG.RELEASE", b"b", &tok])).await,
        "TOKENUSED",
    );
}

#[tokio::test]
async fn release_returns_everything() {
    let (_d, e) = engine();
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    let g = reserve(&e, b"b", "30", &[]).await;
    let tok = token_of(&g);
    assert_eq!(
        int(budget::release(&e, &a(&[b"BG.RELEASE", b"b", &tok])).await),
        30
    );
    assert_eq!(info_field(&e, b"b", "available").await, 100);
}

#[tokio::test]
async fn never_overspends_and_fails_closed() {
    let (_d, e) = engine();
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    let _g = reserve(&e, b"b", "100", &[]).await;
    assert_err_contains(reserve(&e, b"b", "1", &[]).await, "BUDGETEXHAUSTED");
    // A pile of concurrent-ish reservations can never exceed capacity.
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"c", b"50"])).await);
    let mut granted = 0i64;
    for _ in 0..10 {
        match reserve(&e, b"c", "10", &[]).await {
            Reply::Map(_) => granted += 10,
            Reply::Err(err) => assert!(err.contains("BUDGETEXHAUSTED")),
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(granted, 50);
}

#[tokio::test]
async fn amount_and_ttl_bounds() {
    let (_d, e) = engine();
    ok(budget::create(
        &e,
        &a(&[
            b"BG.CREATE",
            b"b",
            b"100",
            b"MAXAMOUNT",
            b"10",
            b"MAXTTL",
            b"60000",
        ]),
    )
    .await);
    assert_err_contains(reserve(&e, b"b", "11", &[]).await, "exceeds budget maximum");
    assert_err_contains(reserve(&e, b"b", "0", &[]).await, "positive");
    assert_err_contains(
        reserve(&e, b"b", "5", &[b"TTL", b"3600001"]).await,
        "TTL must be in",
    );
    // u64-boundary amounts fail closed, never wrap.
    assert_err_contains(
        reserve(&e, b"b", "18446744073709551615", &[]).await,
        "exceeds budget maximum",
    );
}

#[tokio::test]
async fn expired_token_is_reclaimed_and_late_commit_rejected() {
    let (_d, e) = engine();
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    let g = reserve(&e, b"b", "60", &[b"TTL", b"50"]).await;
    let tok = token_of(&g);
    assert_eq!(info_field(&e, b"b", "available").await, 40);
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    // Late COMMIT: the issuer folds the token as expired and refuses the spend.
    assert_err_contains(
        budget::commit(&e, &a(&[b"BG.COMMIT", b"b", &tok, b"60"])).await,
        "TOKENEXPIRED",
    );
    // Full reservation flowed back (auto-reclaim policy).
    assert_eq!(info_field(&e, b"b", "available").await, 100);
}

#[tokio::test]
async fn grant_path_sweeps_expired_tokens() {
    let (_d, e) = engine();
    // Tiny reclaim grace so the sweep fires quickly.
    e.budget_reclaim_grace_ms
        .store(1, std::sync::atomic::Ordering::Relaxed);
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    let _g = reserve(&e, b"b", "100", &[b"TTL", b"40"]).await;
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    // The abandoned token is past deadline+grace: the next grant folds it
    // opportunistically and succeeds.
    let g2 = reserve(&e, b"b", "100", &[]).await;
    assert_eq!(int(map_get(&g2, "amount")), 100);
}

#[tokio::test]
async fn draw_tracks_incremental_spend() {
    let (_d, e) = engine();
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    let g = reserve(&e, b"b", "50", &[]).await;
    let tok = token_of(&g);
    assert_eq!(
        int(budget::draw(&e, &a(&[b"BG.DRAW", b"b", &tok, b"10"])).await),
        40
    );
    assert_err_contains(
        budget::draw(&e, &a(&[b"BG.DRAW", b"b", &tok, b"45"])).await,
        "exceeds reserved",
    );
    // Committing below the drawn amount is contradictory.
    assert_err_contains(
        budget::commit(&e, &a(&[b"BG.COMMIT", b"b", &tok, b"5"])).await,
        "below already drawn",
    );
    // COMMIT with no explicit spent accepts the drawn total.
    assert_eq!(
        int(budget::commit(&e, &a(&[b"BG.COMMIT", b"b", &tok])).await),
        40
    );
    assert_eq!(info_field(&e, b"b", "available").await, 90);
}

#[tokio::test]
async fn wrongtype_and_missing() {
    let (_d, e) = engine();
    string_cmd::set(&e, &a(&[b"SET", b"s", b"v"])).await;
    assert_err_contains(
        budget::create(&e, &a(&[b"BG.CREATE", b"s", b"10"])).await,
        "WRONGTYPE",
    );
    assert_err_contains(budget::info(&e, &a(&[b"BG.INFO", b"s"])).await, "WRONGTYPE");
    assert_err_contains(reserve(&e, b"nope", "1", &[]).await, "NOBUDGET");
    assert_err_contains(
        budget::info(&e, &a(&[b"BG.INFO", b"nope"])).await,
        "NOBUDGET",
    );
    // Token issued by this node id (7) so routing stays local.
    assert_err_contains(
        budget::commit(&e, &a(&[b"BG.COMMIT", b"nope", b"1-2-7-4"])).await,
        "NOBUDGET",
    );
    assert_err_contains(
        budget::commit(&e, &a(&[b"BG.COMMIT", b"nope", b"garbage"])).await,
        "invalid token",
    );
}

#[tokio::test]
async fn generic_commands_are_fenced() {
    let (_d, e) = engine();
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    assert_eq!(
        generic::type_cmd(&e, &a(&[b"TYPE", b"b"])).await,
        Reply::Simple("budget")
    );
    assert_err_contains(
        generic::expire(&e, &a(&[b"EXPIRE", b"b", b"100"]), 1000, false).await,
        "not supported for budget",
    );
    assert_err_contains(
        generic::rename(&e, &a(&[b"RENAME", b"b", b"b2"]), false).await,
        "not supported for budget",
    );
    assert_err_contains(
        generic::copy(&e, &a(&[b"COPY", b"b", b"b2"])).await,
        "not supported for budget",
    );
    // SET overwrites any type in Redis; here the string record SHADOWS the
    // budget: commands fail closed while shadowed, the ledger survives, and
    // deleting the string un-shadows it.
    ok(string_cmd::set(&e, &a(&[b"SET", b"b", b"v"])).await);
    assert_eq!(
        generic::type_cmd(&e, &a(&[b"TYPE", b"b"])).await,
        Reply::Simple("string")
    );
    assert_err_contains(budget::info(&e, &a(&[b"BG.INFO", b"b"])).await, "WRONGTYPE");
    assert_err_contains(reserve(&e, b"b", "1", &[]).await, "WRONGTYPE");
    assert_eq!(int(generic::del(&e, &a(&[b"DEL", b"b"])).await), 1);
    assert_eq!(info_field(&e, b"b", "available").await, 100);
}

#[tokio::test]
async fn del_starts_a_new_generation() {
    let (_d, e) = engine();
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    let g = reserve(&e, b"b", "40", &[]).await;
    let tok = token_of(&g);
    assert_eq!(int(generic::del(&e, &a(&[b"DEL", b"b"])).await), 1);
    assert_err_contains(reserve(&e, b"b", "1", &[]).await, "NOBUDGET");
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    // Old-generation token cannot touch the recreated budget.
    assert_err_contains(
        budget::commit(&e, &a(&[b"BG.COMMIT", b"b", &tok, b"0"])).await,
        "TOKENEXPIRED",
    );
    assert_eq!(info_field(&e, b"b", "available").await, 100);
    assert_eq!(info_field(&e, b"b", "outstanding").await, 0);
}

#[tokio::test]
async fn central_ops_are_seq_idempotent() {
    let (_d, e) = engine();
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100", b"SEQ", b"5"])).await);
    let gen1 = map_get(
        &budget::info(&e, &a(&[b"BG.INFO", b"b"])).await,
        "generation",
    );
    // Retried CREATE with the same SEQ is a no-op: same generation survives.
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"999", b"SEQ", b"5"])).await);
    let info = budget::info(&e, &a(&[b"BG.INFO", b"b"])).await;
    assert_eq!(map_get(&info, "generation"), gen1);
    assert_eq!(int(map_get(&info, "capacity")), 100);

    assert_eq!(
        int(budget::topup(&e, &a(&[b"BG.TOPUP", b"b", b"50", b"SEQ", b"6"])).await),
        150
    );
    // At-least-once retry: second TOPUP with the same SEQ does not re-apply.
    assert_eq!(
        int(budget::topup(&e, &a(&[b"BG.TOPUP", b"b", b"50", b"SEQ", b"6"])).await),
        150
    );
    assert_eq!(info_field(&e, b"b", "capacity").await, 150);
    // Capacity overflow fails closed.
    assert_err_contains(
        budget::topup(&e, &a(&[b"BG.TOPUP", b"b", b"18446744073709551615"])).await,
        "overflow",
    );
}

#[tokio::test]
async fn reqid_dedup_returns_original_token() {
    let (_d, e) = engine();
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    let g1 = reserve(&e, b"b", "10", &[b"REQID", b"42"]).await;
    let g2 = reserve(&e, b"b", "10", &[b"REQID", b"42"]).await;
    assert_eq!(token_of(&g1), token_of(&g2));
    // Only one grant actually happened.
    assert_eq!(info_field(&e, b"b", "available").await, 90);
}

#[tokio::test]
async fn restart_preserves_ledger_and_tokens() {
    let dir = tempfile::tempdir().unwrap();
    let tok;
    {
        let e = open(&dir, 7);
        ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
        let g = reserve(&e, b"b", "40", &[b"TTL", b"60000"]).await;
        tok = token_of(&g);
        assert_eq!(info_field(&e, b"b", "available").await, 60);
        drop(e); // Store::drop closes ondadb cleanly
    }
    let e = open(&dir, 7);
    // Same epoch (persisted) → not a fresh incarnation → grants allowed and
    // the durable ledger still gates them.
    assert!(!e.store.epoch_fresh);
    assert_eq!(info_field(&e, b"b", "available").await, 60);
    assert_err_contains(reserve(&e, b"b", "61", &[]).await, "BUDGETEXHAUSTED");
    assert_eq!(
        int(budget::commit(&e, &a(&[b"BG.COMMIT", b"b", &tok, b"40"])).await),
        0
    );
    assert_eq!(info_field(&e, b"b", "available").await, 60);
}

/// Boot grant-fence: a fresh-epoch node with a peer layer must fetch its own
/// records from a home before granting; an unreachable home fails closed.
#[tokio::test]
async fn boot_fence_fails_closed_without_reachable_home() {
    use marekvs_engine::{BudgetErr, BudgetGrant, BudgetPeer};
    use std::future::Future;
    use std::pin::Pin;

    struct UnreachablePeer;
    impl BudgetPeer for UnreachablePeer {
        fn reserve_remote<'a>(
            &'a self,
            _node: u16,
            _key: &'a [u8],
            _amount: u64,
            _ttl_ms: u64,
            _reqid: u64,
        ) -> Pin<Box<dyn Future<Output = Result<BudgetGrant, BudgetErr>> + Send + 'a>> {
            Box::pin(async { Err(BudgetErr::TryAgain("unreachable")) })
        }
        fn close_remote<'a>(
            &'a self,
            _node: u16,
            _key: &'a [u8],
            _token: &'a [u8],
            _spent: Option<u64>,
            _draw: Option<u64>,
            _release: bool,
        ) -> Pin<Box<dyn Future<Output = Result<u64, BudgetErr>> + Send + 'a>> {
            Box::pin(async { Err(BudgetErr::TryAgain("unreachable")) })
        }
        fn fetch_budget<'a>(
            &'a self,
            _key: &'a [u8],
        ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
            Box::pin(async { false })
        }
        fn owners_for(&self, _key: &[u8]) -> Vec<u16> {
            Vec::new()
        }
    }

    let (_d, e) = engine(); // fresh tempdir ⇒ epoch_fresh = true
    assert!(e.store.epoch_fresh);
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    e.set_budget_peer(Arc::new(UnreachablePeer));
    assert_err_contains(reserve(&e, b"b", "10", &[]).await, "TRYAGAIN");
}

/// Replication convergence: capture the published (ikey, value) ops from a
/// full grant/commit history and apply them to a SECOND store in forward,
/// reverse, and torn (partial txn) orders — every order must converge to the
/// same ledger, and the fold must absorb a stale open-token replay.
#[tokio::test]
async fn published_ops_converge_in_any_order() {
    type Ops = Vec<(Vec<u8>, Vec<u8>)>;
    let (_d, e) = engine();
    let captured: Arc<parking_lot::Mutex<Ops>> = Arc::new(parking_lot::Mutex::new(Vec::new()));
    {
        let captured = captured.clone();
        e.set_budget_publish(Arc::new(move |ops| {
            captured.lock().extend(ops);
        }));
    }
    ok(budget::create(&e, &a(&[b"BG.CREATE", b"b", b"100"])).await);
    let g1 = reserve(&e, b"b", "40", &[]).await;
    let g2 = reserve(&e, b"b", "25", &[]).await;
    let t1 = token_of(&g1);
    let _t2 = token_of(&g2);
    assert_eq!(
        int(budget::commit(&e, &a(&[b"BG.COMMIT", b"b", &t1, b"15"])).await),
        25
    );
    let ops = captured.lock().clone();
    assert!(
        ops.len() >= 5,
        "expected head+slots+tokens, got {}",
        ops.len()
    );

    let apply = |order: Vec<usize>| async {
        let (_d2, e2) = engine();
        let pid = marekvs_core::pid_of(b"b");
        for i in order {
            let (k, v) = ops[i].clone();
            e2.store
                .run(pid, move |ctx| {
                    marekvs_engine::store::write_merged(ctx, &k, &v);
                })
                .await;
        }
        let avail = info_field(&e2, b"b", "available").await;
        let outstanding = info_field(&e2, b"b", "outstanding").await;
        (avail, outstanding)
    };

    let n = ops.len();
    let forward = apply((0..n).collect()).await;
    let reverse = apply((0..n).rev().collect()).await;
    let doubled = apply((0..n).chain(0..n).collect()).await;
    // Torn fold: replay everything, then a stale copy of the pre-fold open
    // token (its first write) again — the rank-2 fold must absorb it.
    let torn = apply((0..n).chain(std::iter::once(1)).collect()).await;
    assert_eq!(forward, reverse);
    assert_eq!(forward, doubled);
    assert_eq!(forward, torn);
    // 40 granted, 15 spent, 25 returned; 25 still open: available = 100-15-25.
    assert_eq!(forward, (60, 40));
}

// ---------------------------------------------------------------------------
// Window mode (MODE WINDOW, design/13 fix 9)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn window_mode_refills_each_period() {
    let (_d, e) = engine();
    ok(budget::create(
        &e,
        &a(&[
            b"BG.CREATE",
            b"w",
            b"100",
            b"MODE",
            b"WINDOW",
            b"400",
            b"MAXTTL",
            b"300",
        ]),
    )
    .await);
    // Fill this window completely.
    let g = reserve(&e, b"w", "100", &[b"TTL", b"200"]).await;
    assert_eq!(int(map_get(&g, "amount")), 100);
    assert_err_contains(
        reserve(&e, b"w", "1", &[b"TTL", b"200"]).await,
        "BUDGETEXHAUSTED",
    );
    // Next window: the full allowance is available again — the previous
    // window's grants never count against it.
    tokio::time::sleep(std::time::Duration::from_millis(450)).await;
    let g2 = reserve(&e, b"w", "100", &[b"TTL", b"200"]).await;
    assert_eq!(int(map_get(&g2, "amount")), 100);
}

#[tokio::test]
async fn window_fold_credits_the_grant_window() {
    let (_d, e) = engine();
    ok(budget::create(
        &e,
        &a(&[
            b"BG.CREATE",
            b"w",
            b"100",
            b"MODE",
            b"WINDOW",
            b"2000",
            b"MAXTTL",
            b"10000",
        ]),
    )
    .await);
    let g = reserve(&e, b"w", "60", &[b"TTL", b"8000"]).await;
    let tok = token_of(&g);
    // Roll into the next window, grant its full allowance, THEN commit the
    // old-window token: the credit lands in the OLD window's ledger and must
    // not free headroom in the current one. (Wide window: the remaining
    // assertions run well inside it even on a loaded machine.)
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    let g2 = reserve(&e, b"w", "100", &[b"TTL", b"200"]).await;
    assert_eq!(int(map_get(&g2, "amount")), 100);
    assert_eq!(
        int(budget::commit(&e, &a(&[b"BG.COMMIT", b"w", &tok, b"10"])).await),
        50
    );
    assert_err_contains(
        reserve(&e, b"w", "1", &[b"TTL", b"200"]).await,
        "BUDGETEXHAUSTED",
    );
}

#[tokio::test]
async fn old_window_slots_are_garbage_collected() {
    let (_d, e) = engine();
    e.budget_reclaim_grace_ms
        .store(1, std::sync::atomic::Ordering::Relaxed);
    ok(budget::create(
        &e,
        &a(&[
            b"BG.CREATE",
            b"w",
            b"100",
            b"MODE",
            b"WINDOW",
            b"100",
            b"MAXTTL",
            b"50",
            b"TTL",
            b"50",
        ]),
    )
    .await);
    // Leave a slot record in several early windows.
    for _ in 0..3 {
        let _ = reserve(&e, b"w", "5", &[]).await;
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    }
    // retention = (50 + 2*1)/100 + 2 = 2 windows; run far past it, then a
    // grant sweeps GC.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let _ = reserve(&e, b"w", "5", &[]).await;
    let live = e
        .store
        .run_key(b"w", |ctx| {
            let prefix = {
                let mut s = vec![marekvs_core::ikey::BUDGET_WINDOW_SLOT];
                // count LIVE window slots across all windows of any gen: scan
                // the whole budget kind space for this key
                s.clear();
                s.push(marekvs_core::ikey::BUDGET_WINDOW_SLOT);
                marekvs_core::ikey::prefixed(marekvs_core::ikey::Tag::Budget, b"w", &s)
            };
            let mut live = 0;
            marekvs_engine::store::scan_prefix(ctx, &prefix, |_k, v| {
                if marekvs_core::Envelope::decode(v).is_some_and(|(e, _)| !e.is_tombstone()) {
                    live += 1;
                }
                true
            });
            live
        })
        .await;
    // The early windows' slots must be tombstoned; only recent ones remain.
    assert!(
        live <= 3,
        "expected old window slots GC'd, {live} still live"
    );
}
