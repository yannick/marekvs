use std::sync::Arc;

use marekvs_engine::cmd::{generic, hash, list, stream, string as string_cmd, zset};
use marekvs_engine::reply::Reply;
use marekvs_engine::store::{Store, StoreConfig};
use marekvs_engine::Engine;

fn engine() -> (tempfile::TempDir, Arc<Engine>) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        node_id: 7,
        shard_threads: 2,
        ..StoreConfig::default()
    })
    .unwrap();
    (dir, Engine::new(store))
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

fn bulk(r: Reply) -> Vec<u8> {
    match r {
        Reply::Bulk(v) => v,
        other => panic!("expected Bulk, got {other:?}"),
    }
}

fn array(r: Reply) -> Vec<Reply> {
    match r {
        Reply::Array(v) => v,
        other => panic!("expected Array, got {other:?}"),
    }
}

fn bulk_list(r: Reply) -> Vec<Vec<u8>> {
    array(r).into_iter().map(bulk).collect()
}

fn assert_err_contains(r: Reply, needle: &str) {
    match r {
        Reply::Err(e) => assert!(e.contains(needle), "error {e:?} did not contain {needle:?}"),
        other => panic!("expected Err containing {needle:?}, got {other:?}"),
    }
}

fn info_value(info: &[Reply], key: &[u8]) -> Reply {
    info.chunks(2)
        .find_map(|pair| match pair {
            [Reply::Bulk(k), v] if k == key => Some(v.clone()),
            _ => None,
        })
        .unwrap_or_else(|| panic!("missing xinfo field {}", String::from_utf8_lossy(key)))
}

#[tokio::test]
async fn lmpop_returns_first_non_empty_list_with_count() {
    let (_dir, e) = engine();
    assert_eq!(
        int(list::push(&e, &a(&[b"RPUSH", b"b", b"1", b"2", b"3"]), false, false).await),
        3
    );

    let out = array(
        list::lmpop(
            &e,
            &a(&[b"LMPOP", b"2", b"a", b"b", b"LEFT", b"COUNT", b"2"]),
        )
        .await,
    );
    assert_eq!(bulk(out[0].clone()), b"b".to_vec());
    let vals = array(out[1].clone())
        .into_iter()
        .map(bulk)
        .collect::<Vec<_>>();
    assert_eq!(vals, [b"1".to_vec(), b"2".to_vec()]);
}

#[tokio::test]
async fn lmpop_and_blmpop_cover_direction_timeout_and_errors() {
    let (_dir, e) = engine();
    assert_eq!(
        int(list::push(&e, &a(&[b"RPUSH", b"a", b"1", b"2", b"3"]), false, false).await),
        3
    );

    let right = array(list::lmpop(&e, &a(&[b"LMPOP", b"1", b"a", b"RIGHT"])).await);
    assert_eq!(bulk(right[0].clone()), b"a".to_vec());
    assert_eq!(bulk_list(right[1].clone()), [b"3".to_vec()]);
    assert_eq!(
        bulk_list(list::lrange(&e, &a(&[b"LRANGE", b"a", b"0", b"-1"])).await),
        [b"1".to_vec(), b"2".to_vec()]
    );

    let hit = array(
        list::blmpop(
            &e,
            &a(&[b"BLMPOP", b"0.001", b"1", b"a", b"LEFT", b"COUNT", b"2"]),
        )
        .await,
    );
    assert_eq!(bulk(hit[0].clone()), b"a".to_vec());
    assert_eq!(bulk_list(hit[1].clone()), [b"1".to_vec(), b"2".to_vec()]);
    assert!(matches!(
        list::blmpop(&e, &a(&[b"BLMPOP", b"0.001", b"1", b"a", b"LEFT"])).await,
        Reply::NullArray
    ));
    assert_err_contains(
        list::lmpop(&e, &a(&[b"LMPOP", b"1", b"a", b"LEFT", b"COUNT", b"0"])).await,
        "count should be greater than 0",
    );
    assert!(matches!(
        list::blmpop(&e, &a(&[b"BLMPOP", b"-1", b"1", b"a", b"LEFT"])).await,
        Reply::Err(ref e) if e.contains("timeout")
    ));
}

#[tokio::test]
async fn zset_new_pop_and_lex_commands() {
    let (_dir, e) = engine();
    assert_eq!(
        int(zset::zadd(&e, &a(&[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"])).await),
        3
    );
    assert_eq!(
        int(zset::zlexcount(&e, &a(&[b"ZLEXCOUNT", b"z", b"[a", b"[c"])).await),
        3
    );
    let popped = array(zset::zmpop(&e, &a(&[b"ZMPOP", b"1", b"z", b"MIN", b"COUNT", b"2"])).await);
    assert_eq!(bulk(popped[0].clone()), b"z".to_vec());
    let members = array(popped[1].clone());
    assert_eq!(bulk(members[0].clone()), b"a".to_vec());
    assert_eq!(bulk(members[2].clone()), b"b".to_vec());
}

#[tokio::test]
async fn zset_lex_range_variants_and_removals() {
    let (_dir, e) = engine();
    let mut cmd = vec![b"ZADD".to_vec(), b"lex".to_vec()];
    for member in [
        "alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega",
    ] {
        cmd.push(b"0".to_vec());
        cmd.push(member.as_bytes().to_vec());
    }
    assert_eq!(int(zset::zadd(&e, &cmd).await), 9);

    assert_eq!(
        bulk_list(
            zset::zrangebylex(&e, &a(&[b"ZRANGEBYLEX", b"lex", b"-", b"[cool"]), false).await
        ),
        [b"alpha".to_vec(), b"bar".to_vec(), b"cool".to_vec()]
    );
    assert_eq!(
        bulk_list(
            zset::zrangebylex(
                &e,
                &a(&[
                    b"ZRANGEBYLEX",
                    b"lex",
                    b"[bar",
                    b"[down",
                    b"LIMIT",
                    b"1",
                    b"2"
                ]),
                false,
            )
            .await
        ),
        [b"cool".to_vec(), b"down".to_vec()]
    );
    assert_eq!(
        bulk_list(
            zset::zrangebylex(&e, &a(&[b"ZREVRANGEBYLEX", b"lex", b"+", b"(great"]), true).await
        ),
        [b"omega".to_vec(), b"hill".to_vec()]
    );
    assert_eq!(
        int(zset::zlexcount(&e, &a(&[b"ZLEXCOUNT", b"lex", b"(bar", b"[foo"])).await),
        4
    );
    assert_eq!(
        int(zset::zremrangebylex(&e, &a(&[b"ZREMRANGEBYLEX", b"lex", b"[bar", b"(foo"])).await),
        4
    );
    assert_eq!(
        bulk_list(zset::zrange(&e, &a(&[b"ZRANGE", b"lex", b"0", b"-1"])).await),
        [
            b"alpha".to_vec(),
            b"foo".to_vec(),
            b"great".to_vec(),
            b"hill".to_vec(),
            b"omega".to_vec()
        ]
    );
    assert!(matches!(
        zset::zlexcount(&e, &a(&[b"ZLEXCOUNT", b"lex", b"foo", b"[bar"])).await,
        Reply::Err(ref e) if e == "ERR syntax error"
    ));
}

#[tokio::test]
async fn zset_rank_store_random_and_set_operations() {
    let (_dir, e) = engine();
    assert_eq!(
        int(zset::zadd(
            &e,
            &a(&[b"ZADD", b"za", b"1", b"a", b"2", b"b", b"3", b"c"])
        )
        .await),
        3
    );
    assert_eq!(
        int(zset::zadd(
            &e,
            &a(&[b"ZADD", b"zb", b"1", b"b", b"2", b"c", b"3", b"d"])
        )
        .await),
        3
    );

    let sample =
        array(zset::zrandmember(&e, &a(&[b"ZRANDMEMBER", b"za", b"2", b"WITHSCORES"])).await);
    assert_eq!(bulk(sample[0].clone()), b"a".to_vec());
    assert!(matches!(sample[1], Reply::Double(1.0)));
    assert_eq!(bulk(sample[2].clone()), b"b".to_vec());
    assert!(matches!(sample[3], Reply::Double(2.0)));

    assert_eq!(
        int(zset::zrangestore(&e, &a(&[b"ZRANGESTORE", b"slice", b"za", b"1", b"2"])).await),
        2
    );
    assert_eq!(
        bulk_list(zset::zrange(&e, &a(&[b"ZRANGE", b"slice", b"0", b"-1"])).await),
        [b"b".to_vec(), b"c".to_vec()]
    );
    assert_eq!(
        int(zset::zremrangebyrank(&e, &a(&[b"ZREMRANGEBYRANK", b"slice", b"-1", b"-1"])).await),
        1
    );
    assert_eq!(
        bulk_list(zset::zrange(&e, &a(&[b"ZRANGE", b"slice", b"0", b"-1"])).await),
        [b"b".to_vec()]
    );

    let union = array(
        zset::zsetop(
            &e,
            &a(&[
                b"ZUNION",
                b"2",
                b"za",
                b"zb",
                b"WEIGHTS",
                b"2",
                b"3",
                b"WITHSCORES",
            ]),
            zset::ZSetOp::Union,
            false,
        )
        .await,
    );
    assert_eq!(bulk(union[0].clone()), b"a".to_vec());
    assert!(matches!(union[1], Reply::Double(2.0)));
    assert_eq!(bulk(union[2].clone()), b"b".to_vec());
    assert!(matches!(union[3], Reply::Double(7.0)));
    assert_eq!(bulk(union[6].clone()), b"c".to_vec());
    assert!(matches!(union[7], Reply::Double(12.0)));

    assert_eq!(
        int(zset::zintercard(&e, &a(&[b"ZINTERCARD", b"2", b"za", b"zb", b"LIMIT", b"1"])).await),
        1
    );
    assert_eq!(
        int(zset::zsetop(
            &e,
            &a(&[b"ZDIFFSTORE", b"only_a", b"2", b"za", b"zb"]),
            zset::ZSetOp::Diff,
            true,
        )
        .await),
        1
    );
    assert_eq!(
        bulk_list(zset::zrange(&e, &a(&[b"ZRANGE", b"only_a", b"0", b"-1"])).await),
        [b"a".to_vec()]
    );
}

#[tokio::test]
async fn zset_blocking_pop_variants_timeout_and_hit() {
    let (_dir, e) = engine();
    assert!(matches!(
        zset::bzmpop(&e, &a(&[b"BZMPOP", b"0.001", b"1", b"empty", b"MIN"])).await,
        Reply::NullArray
    ));
    assert_eq!(
        int(zset::zadd(&e, &a(&[b"ZADD", b"bz", b"1", b"a", b"2", b"b"])).await),
        2
    );
    let popmax = array(zset::bzpop(&e, &a(&[b"BZPOPMAX", b"bz", b"0.001"]), true).await);
    assert_eq!(bulk(popmax[0].clone()), b"bz".to_vec());
    assert_eq!(bulk(popmax[1].clone()), b"b".to_vec());
    assert!(matches!(popmax[2], Reply::Double(2.0)));
    assert!(matches!(
        zset::bzpop(&e, &a(&[b"BZPOPMIN", b"bz", b"-1"]), false).await,
        Reply::Err(ref e) if e.contains("timeout")
    ));
}

#[tokio::test]
async fn hash_field_expiry_and_hgetdel() {
    let (_dir, e) = engine();
    assert_eq!(
        int(hash::hset(&e, &a(&[b"HSET", b"h", b"f", b"v", b"g", b"w"]), false).await),
        2
    );
    let statuses = array(
        hash::hexpire(
            &e,
            &a(&[b"HEXPIRE", b"h", b"60", b"FIELDS", b"2", b"f", b"missing"]),
            1000,
            false,
        )
        .await,
    )
    .into_iter()
    .map(int)
    .collect::<Vec<_>>();
    assert_eq!(statuses, [1, -2]);

    let ttl = array(
        hash::httl(
            &e,
            &a(&[b"HTTL", b"h", b"FIELDS", b"1", b"f"]),
            false,
            false,
        )
        .await,
    );
    assert!(matches!(ttl[0], Reply::Int(n) if n > 0));
    assert!(matches!(
        hash::hgetex(&e, &a(&[b"HGETEX", b"h", b"KEEPTTL", b"FIELDS", b"1", b"f"])).await,
        Reply::Err(ref e) if e == "ERR syntax error"
    ));
    assert_eq!(
        int(hash::hsetex(
            &e,
            &a(&[b"HSETEX", b"h", b"KEEPTTL", b"FVS", b"1", b"g", b"ww"])
        )
        .await),
        1
    );

    let values =
        array(hash::hgetdel(&e, &a(&[b"HGETDEL", b"h", b"FIELDS", b"2", b"f", b"g"])).await);
    assert_eq!(bulk(values[0].clone()), b"v".to_vec());
    assert_eq!(bulk(values[1].clone()), b"ww".to_vec());
    assert!(matches!(
        hash::hget(&e, &a(&[b"HGET", b"h", b"f"])).await,
        Reply::Null
    ));
}

#[tokio::test]
async fn hash_field_ttl_modes_statuses_and_set_conditions() {
    let (_dir, e) = engine();
    assert_eq!(
        int(hash::hset(&e, &a(&[b"HSET", b"h", b"a", b"1", b"b", b"2"]), false).await),
        2
    );
    assert_eq!(
        array(
            hash::httl(
                &e,
                &a(&[b"HTTL", b"h", b"FIELDS", b"3", b"a", b"b", b"missing"]),
                false,
                false
            )
            .await
        )
        .into_iter()
        .map(int)
        .collect::<Vec<_>>(),
        [-1, -1, -2]
    );
    assert_eq!(
        array(
            hash::hexpire(
                &e,
                &a(&[
                    b"HPEXPIRE",
                    b"h",
                    b"5000",
                    b"NX",
                    b"FIELDS",
                    b"2",
                    b"a",
                    b"b"
                ]),
                1,
                false
            )
            .await
        )
        .into_iter()
        .map(int)
        .collect::<Vec<_>>(),
        [1, 1]
    );
    assert_eq!(
        array(
            hash::hexpire(
                &e,
                &a(&[b"HEXPIRE", b"h", b"1", b"NX", b"FIELDS", b"1", b"a"]),
                1000,
                false
            )
            .await
        )
        .into_iter()
        .map(int)
        .collect::<Vec<_>>(),
        [0]
    );
    assert_eq!(
        array(
            hash::hpersist(
                &e,
                &a(&[b"HPERSIST", b"h", b"FIELDS", b"2", b"a", b"missing"])
            )
            .await
        )
        .into_iter()
        .map(int)
        .collect::<Vec<_>>(),
        [1, -2]
    );
    let ttls = array(
        hash::httl(
            &e,
            &a(&[b"HTTL", b"h", b"FIELDS", b"2", b"a", b"b"]),
            false,
            false,
        )
        .await,
    );
    assert!(matches!(ttls[0], Reply::Int(-1)));
    assert!(matches!(ttls[1], Reply::Int(n) if n > 0 && n <= 5));
    assert_eq!(
        int(hash::hsetex(&e, &a(&[b"HSETEX", b"h", b"FNX", b"FVS", b"1", b"a", b"x"])).await),
        0
    );
    assert_eq!(
        int(hash::hsetex(
            &e,
            &a(&[b"HSETEX", b"h", b"FXX", b"PX", b"5000", b"FVS", b"1", b"c", b"3"])
        )
        .await),
        0
    );
    assert_eq!(
        int(hash::hsetex(
            &e,
            &a(&[b"HSETEX", b"h", b"FXX", b"PX", b"5000", b"FVS", b"1", b"b", b"22"])
        )
        .await),
        1
    );
    assert_eq!(
        bulk_list(
            hash::hgetex(
                &e,
                &a(&[b"HGETEX", b"h", b"PERSIST", b"FIELDS", b"1", b"b"])
            )
            .await
        ),
        [b"22".to_vec()]
    );
    assert_eq!(
        int(array(
            hash::httl(
                &e,
                &a(&[b"HTTL", b"h", b"FIELDS", b"1", b"b"]),
                false,
                false,
            )
            .await,
        )
        .remove(0)),
        -1
    );
}

#[tokio::test]
async fn xsetid_xinfo_and_copy_object() {
    let (_dir, e) = engine();
    assert!(matches!(
        stream::xadd(&e, &a(&[b"XADD", b"s", b"*", b"f", b"v"])).await,
        Reply::Bulk(_)
    ));
    assert!(matches!(
        stream::xsetid(
            &e,
            &a(&[b"XSETID", b"s", b"9999999999999-0", b"ENTRIESADDED", b"10"])
        )
        .await,
        Reply::Simple("OK")
    ));
    let info = array(stream::xinfo(&e, &a(&[b"XINFO", b"STREAM", b"s"])).await);
    assert!(info.windows(2).any(
        |w| matches!((&w[0], &w[1]), (Reply::Bulk(k), Reply::Int(10)) if k == b"entries-added")
    ));

    assert_eq!(int(generic::copy(&e, &a(&[b"COPY", b"s", b"s2"])).await), 1);
    assert!(matches!(
        generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"s2"])).await,
        Reply::Bulk(_)
    ));
}

#[tokio::test]
async fn copy_preserves_values_ttls_and_replace_rules() {
    let (_dir, e) = engine();
    assert!(matches!(
        string_cmd::set(&e, &a(&[b"SET", b"src", b"v", b"PX", b"5000"])).await,
        Reply::Simple("OK")
    ));
    assert!(matches!(
        string_cmd::set(&e, &a(&[b"SET", b"dst", b"old"])).await,
        Reply::Simple("OK")
    ));
    assert_eq!(
        int(generic::copy(&e, &a(&[b"COPY", b"src", b"dst"])).await),
        0
    );
    assert_eq!(
        bulk(string_cmd::get(&e, &a(&[b"GET", b"dst"])).await),
        b"old".to_vec()
    );
    assert_eq!(
        int(generic::copy(&e, &a(&[b"COPY", b"src", b"dst", b"REPLACE"])).await),
        1
    );
    assert_eq!(
        bulk(string_cmd::get(&e, &a(&[b"GET", b"dst"])).await),
        b"v".to_vec()
    );
    assert!(matches!(
        generic::ttl(&e, &a(&[b"PTTL", b"dst"]), true).await,
        Reply::Int(n) if n > 0 && n <= 5000
    ));
    assert!(matches!(
        generic::copy(&e, &a(&[b"COPY", b"src", b"x", b"DB", b"notanumber"])).await,
        Reply::Err(ref e) if e.contains("integer")
    ));
    assert!(matches!(
        generic::copy(&e, &a(&[b"COPY", b"src", b"x", b"DB", b"1"])).await,
        Reply::Err(ref e) if e.contains("out of range")
    ));

    assert_eq!(
        int(hash::hset(&e, &a(&[b"HSET", b"hs", b"f", b"v"]), false).await),
        1
    );
    assert_eq!(
        array(
            hash::hexpire(
                &e,
                &a(&[b"HPEXPIRE", b"hs", b"5000", b"FIELDS", b"1", b"f"]),
                1,
                false
            )
            .await
        )
        .into_iter()
        .map(int)
        .collect::<Vec<_>>(),
        [1]
    );
    assert_eq!(
        int(generic::copy(&e, &a(&[b"COPY", b"hs", b"hs2"])).await),
        1
    );
    assert_eq!(
        bulk(hash::hget(&e, &a(&[b"HGET", b"hs2", b"f"])).await),
        b"v".to_vec()
    );
    assert!(matches!(
        hash::httl(&e, &a(&[b"HPTTL", b"hs2", b"FIELDS", b"1", b"f"]), true, false).await,
        Reply::Array(ref vals) if matches!(vals[0], Reply::Int(n) if n > 0 && n <= 5000)
    ));
}

#[tokio::test]
async fn object_reports_metadata_and_errors() {
    let (_dir, e) = engine();
    assert_eq!(
        int(list::push(&e, &a(&[b"LPUSH", b"l", b"x"]), true, false).await),
        1
    );
    assert_eq!(
        bulk(generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"l"])).await),
        b"quicklist".to_vec()
    );
    assert_eq!(
        int(generic::object(&e, &a(&[b"OBJECT", b"REFCOUNT", b"l"])).await),
        1
    );
    assert_eq!(
        int(generic::object(&e, &a(&[b"OBJECT", b"IDLETIME", b"l"])).await),
        0
    );
    assert_err_contains(
        generic::object(&e, &a(&[b"OBJECT", b"ENCODING", b"missing"])).await,
        "no such key",
    );
    assert_err_contains(
        generic::object(&e, &a(&[b"OBJECT", b"FREQ", b"l"])).await,
        "LFU",
    );
    assert!(matches!(
        generic::object(&e, &a(&[b"OBJECT", b"HELP"])).await,
        Reply::Array(ref rows) if rows.len() >= 5
    ));
}

#[tokio::test]
async fn stream_xsetid_xinfo_errors_and_deletion_metadata() {
    let (_dir, e) = engine();
    assert_err_contains(
        stream::xsetid(&e, &a(&[b"XSETID", b"missing", b"1-1"])).await,
        "no such key",
    );
    assert!(matches!(
        stream::xadd(&e, &a(&[b"XADD", b"s", b"10-1", b"f", b"v"])).await,
        Reply::Bulk(_)
    ));
    assert_err_contains(
        stream::xsetid(&e, &a(&[b"XSETID", b"s", b"1-1"])).await,
        "smaller",
    );
    assert_eq!(
        int(stream::xdel(&e, &a(&[b"XDEL", b"s", b"10-1"])).await),
        1
    );
    assert!(matches!(
        stream::xsetid(
            &e,
            &a(&[
                b"XSETID",
                b"s",
                b"20-0",
                b"ENTRIESADDED",
                b"7",
                b"MAXDELETEDID",
                b"10-1"
            ])
        )
        .await,
        Reply::Simple("OK")
    ));
    let info = array(stream::xinfo(&e, &a(&[b"XINFO", b"STREAM", b"s"])).await);
    assert!(matches!(info_value(&info, b"length"), Reply::Int(0)));
    assert_eq!(
        bulk(info_value(&info, b"last-generated-id")),
        b"20-0".to_vec()
    );
    assert_eq!(
        bulk(info_value(&info, b"max-deleted-entry-id")),
        b"10-1".to_vec()
    );
    assert!(matches!(info_value(&info, b"entries-added"), Reply::Int(7)));
}
