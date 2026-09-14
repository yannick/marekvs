//! Disposable, release-mode idle CPU fixture; see tests/idle_maintenance/README.md.
use marekvs_core::{
    envelope::{Envelope, RecordType},
    ikey,
};
use marekvs_engine::{
    store::{Store, StoreConfig},
    Engine,
};
use ondadb::{ColumnFamilyConfig, Options, DB};
use std::time::{Duration, Instant};

fn cpu_seconds() -> f64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: getrusage initializes the correctly sized output on success.
    assert_eq!(
        unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) },
        0
    );
    let usage = unsafe { usage.assume_init() };
    usage.ru_utime.tv_sec as f64
        + usage.ru_stime.tv_sec as f64
        + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1_000_000.0
}

fn main() {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .map(|s| s.parse().unwrap())
        .collect();
    assert_eq!(
        args.len(),
        6,
        "usage: idle_maintenance SHARDS RANGES DISTINCT_RANGES KEYS WARM_SECONDS SAMPLE_SECONDS"
    );
    let (shards, ranges, distinct, keys, warm, seconds) =
        (args[0], args[1], args[2], args[3], args[4], args[5]);
    assert!(
        shards > 0
            && shards <= 4096
            && distinct <= 4096
            && (ranges == 0 || distinct > 0)
            && seconds > 0
    );
    let dir = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        let cf = db
            .create_column_family("data", ColumnFamilyConfig::default())
            .unwrap();
        db.enable_format_capabilities(ondadb::format::CAP_RANGE_DELETES)
            .unwrap();
        for i in 0..ranges {
            let pid = (i % distinct) as u16;
            db.delete_range(&cf, &pid.to_be_bytes(), &(pid + 1).to_be_bytes())
                .unwrap();
        }
        for i in 0..keys {
            let key = format!("idle-key-{i}");
            let env = Envelope {
                flags: RecordType::String as u8,
                hlc: 1 << 16,
                origin: 1,
                ttl_deadline_ms: 0,
            };
            db.put(
                &cf,
                &ikey::string_key(key.as_bytes()),
                &env.encode_with(b"value"),
                Duration::ZERO,
            )
            .unwrap();
        }
    }
    let store = Store::open(&StoreConfig {
        data_dir: dir.path().to_string_lossy().into_owned(),
        shard_threads: shards,
        ..StoreConfig::default()
    })
    .unwrap();
    let engine = Engine::new(store);
    std::thread::sleep(Duration::from_secs(warm as u64));
    println!(
        "BEGIN_METRICS\n{}",
        engine.metrics.render(engine.started_at_ms, 0)
    );
    let wall = Instant::now();
    let cpu = cpu_seconds();
    std::thread::sleep(Duration::from_secs(seconds as u64));
    let cpu = cpu_seconds() - cpu;
    let wall = wall.elapsed().as_secs_f64();
    println!(
        "END_METRICS\n{}",
        engine.metrics.render(engine.started_at_ms, 0)
    );
    println!("RESULT shards={shards} ranges={ranges} distinct={distinct} keys={keys} warm_seconds={warm} sample_seconds={wall:.3} cpu_seconds={cpu:.6} cpu_percent={:.3}", cpu / wall * 100.0);
}
