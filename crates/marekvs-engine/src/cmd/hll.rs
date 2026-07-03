//! HyperLogLog family (PFADD / PFCOUNT / PFMERGE) — design/02 §HyperLogLog.
//!
//! The sketch is decomposed into ONE RECORD PER TOUCHED REGISTER under a
//! head-gated collection (ctype `CTYPE_HLL`), the same shape as our other
//! collections:
//!
//! * PFADD writes at most one ~30-byte register record per element —
//!   replication ships natural deltas, never a 12 KiB sketch blob; a no-op
//!   add (rank not exceeded) writes and replicates NOTHING (the register
//!   merge is a monotone max, so `write_merged` reports KeepLocal).
//! * DEL is one head tombstone; the head delete clock gates old registers,
//!   giving resurrection safety without epochs (identical to sets).
//! * Anti-entropy digests per register, so repair ships only divergent
//!   registers.
//!
//! ## Frozen parameters — cluster-wide wire/storage contract
//! `P = 14` (m = 16384 registers) and the element hash `xxh3_64` are
//! FROZEN: every node must map an element to the identical (bucket, rank)
//! forever. Changing either silently corrupts every estimate in a rolling
//! upgrade. Treat like a wire format.
//!
//! Estimator: classic HLL (Flajolet et al.) with the linear-counting
//! small-range correction; 64-bit hashes need no large-range correction.
//! Standard error ≈ 1.04/√m ≈ 0.81 % (Redis-grade minus its empirical
//! bias table in the 2.5m–5m band; documented in design/02).

use std::sync::Arc;

use crate::reply::Reply;
use crate::store::{
    check_type, ensure_head, get_head, now_ms, scan_prefix, visible, write_merged, ShardCtx,
};
use crate::Engine;
use marekvs_core::envelope::{head, Envelope, RecordType};
use marekvs_core::ikey;
use xxhash_rust::xxh3::xxh3_64;

/// log2 of the register count. FROZEN — see module docs.
pub const P: u32 = 14;
pub const M: usize = 1 << P; // 16384

fn alpha() -> f64 {
    // α_m for m ≥ 128 (Flajolet et al.).
    0.7213 / (1.0 + 1.079 / M as f64)
}

/// Element → (bucket, rank). Bucket = low P bits; rank = leading-zero count
/// of the remaining 64-P bits + 1 (all-zero remainder → max rank 51).
pub fn bucket_rank(element: &[u8]) -> (u16, u8) {
    let h = xxh3_64(element);
    let bucket = (h & (M as u64 - 1)) as u16;
    let w = h >> P;
    let rank = if w == 0 {
        (64 - P + 1) as u8
    } else {
        (w.leading_zeros() - P + 1) as u8
    };
    (bucket, rank)
}

fn hll_del_hlc(ctx: &ShardCtx, key: &[u8]) -> Result<u64, ()> {
    check_type(ctx, key, head::CTYPE_HLL)
}

/// Read all live registers into a dense array (0 = untouched).
fn load_registers(ctx: &ShardCtx, key: &[u8], del: u64, regs: &mut [u8; M]) {
    let now = now_ms();
    scan_prefix(
        ctx,
        &ikey::collection_prefix(ikey::Tag::HllRegister, key),
        |k, v| {
            if let (Some(p), Some((env, pay))) = (ikey::parse(k), Envelope::decode(v)) {
                if visible(&env, pay, del, now).is_some() && p.suffix.len() == 2 {
                    let bucket = u16::from_be_bytes([p.suffix[0], p.suffix[1]]) as usize;
                    if bucket < M {
                        if let Some(&rank) = pay.first() {
                            regs[bucket] = regs[bucket].max(rank);
                        }
                    }
                }
            }
            true
        },
    );
}

/// Cardinality estimate from a dense register array.
pub fn estimate(regs: &[u8; M]) -> u64 {
    let mut inv_sum = 0.0f64;
    let mut zeros = 0usize;
    for &r in regs.iter() {
        inv_sum += 1.0 / (1u64 << r.min(63)) as f64;
        if r == 0 {
            zeros += 1;
        }
    }
    let raw = alpha() * (M as f64) * (M as f64) / inv_sum;
    // Small-range correction: linear counting while raw ≤ 2.5 m and any
    // register is still zero.
    let est = if raw <= 2.5 * M as f64 && zeros > 0 {
        (M as f64) * ((M as f64) / zeros as f64).ln()
    } else {
        raw
    };
    est.round() as u64
}

/// Write one register (max-merge). Returns true if the stored rank rose.
fn write_register(ctx: &ShardCtx, key: &[u8], bucket: u16, rank: u8) -> bool {
    let rec =
        Envelope::new(RecordType::HllRegister, ctx.hlc.now(), ctx.node_id).encode_with(&[rank]);
    write_merged(ctx, &ikey::hll_register_key(key, bucket), &rec)
}

/// PFADD key [element ...] → 1 if the sketch changed (any register rose or
/// the key was created), else 0.
pub async fn pfadd(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("pfadd");
    }
    let key = args[1].clone();
    let updates: Vec<(u16, u8)> = args[2..].iter().map(|e| bucket_rank(e)).collect();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if hll_del_hlc(ctx, &key).is_err() {
                return Reply::wrongtype();
            }
            let created = get_head(ctx, &key).is_none_or(|(env, t, _)| {
                env.is_tombstone() || env.is_expired(now_ms()) || t != head::CTYPE_HLL
            });
            ensure_head(ctx, &key, head::CTYPE_HLL);
            let mut changed = created;
            for (bucket, rank) in &updates {
                if write_register(ctx, &key, *bucket, *rank) {
                    changed = true;
                }
            }
            Reply::Int(changed as i64)
        })
        .await
}

/// PFCOUNT key [key ...] → estimated cardinality (union across keys).
pub async fn pfcount(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("pfcount");
    }
    let mut regs = Box::new([0u8; M]);
    for keyarg in &args[1..] {
        engine.ensure_local(keyarg).await;
        let key = keyarg.clone();
        let partial = engine
            .store
            .run_key(keyarg, move |ctx| {
                let del = match hll_del_hlc(ctx, &key) {
                    Ok(d) => d,
                    Err(()) => return Err(()),
                };
                let mut r = Box::new([0u8; M]);
                load_registers(ctx, &key, del, &mut r);
                Ok(r)
            })
            .await;
        match partial {
            Err(()) => return Reply::wrongtype(),
            Ok(r) => {
                for (dst, src) in regs.iter_mut().zip(r.iter()) {
                    *dst = (*dst).max(*src);
                }
            }
        }
    }
    Reply::Int(estimate(&regs) as i64)
}

/// PFMERGE dst [src ...] → union all sources into dst.
pub async fn pfmerge(engine: &Arc<Engine>, args: &[Vec<u8>]) -> Reply {
    if args.len() < 2 {
        return Reply::wrong_args("pfmerge");
    }
    // Union the sources (dst's own registers merge implicitly at write).
    let mut union = Box::new([0u8; M]);
    for keyarg in &args[2..] {
        engine.ensure_local(keyarg).await;
        let key = keyarg.clone();
        let partial = engine
            .store
            .run_key(keyarg, move |ctx| {
                let del = match hll_del_hlc(ctx, &key) {
                    Ok(d) => d,
                    Err(()) => return Err(()),
                };
                let mut r = Box::new([0u8; M]);
                load_registers(ctx, &key, del, &mut r);
                Ok(r)
            })
            .await;
        match partial {
            Err(()) => return Reply::wrongtype(),
            Ok(r) => {
                for (dst, src) in union.iter_mut().zip(r.iter()) {
                    *dst = (*dst).max(*src);
                }
            }
        }
    }
    let dst = args[1].clone();
    engine
        .store
        .run_key(&args[1], move |ctx| {
            if hll_del_hlc(ctx, &dst).is_err() {
                return Reply::wrongtype();
            }
            ensure_head(ctx, &dst, head::CTYPE_HLL);
            for (bucket, &rank) in union.iter().enumerate() {
                if rank > 0 {
                    write_register(ctx, &dst, bucket as u16, rank);
                }
            }
            Reply::ok()
        })
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_rank_deterministic_and_bounded() {
        let (b1, r1) = bucket_rank(b"hello");
        let (b2, r2) = bucket_rank(b"hello");
        assert_eq!((b1, r1), (b2, r2));
        assert!((b1 as usize) < M);
        assert!(r1 >= 1 && r1 <= (64 - P + 1) as u8);
    }

    #[test]
    fn estimator_accuracy() {
        // 100k distinct elements → estimate within 3 % (σ ≈ 0.81 %).
        let mut regs = Box::new([0u8; M]);
        let n = 100_000u64;
        for i in 0..n {
            let (b, r) = bucket_rank(format!("elem-{i}").as_bytes());
            regs[b as usize] = regs[b as usize].max(r);
        }
        let est = estimate(&regs);
        let err = (est as f64 - n as f64).abs() / n as f64;
        assert!(err < 0.03, "estimate {est} vs {n} (err {err:.4})");
    }

    #[test]
    fn estimator_small_range() {
        // Linear counting regime: small sets should be near-exact.
        let mut regs = Box::new([0u8; M]);
        for i in 0..100u32 {
            let (b, r) = bucket_rank(format!("s{i}").as_bytes());
            regs[b as usize] = regs[b as usize].max(r);
        }
        let est = estimate(&regs);
        assert!((95..=105).contains(&est), "small-range estimate {est}");
    }

    #[test]
    fn empty_estimates_zero() {
        let regs = Box::new([0u8; M]);
        assert_eq!(estimate(&regs), 0);
    }
}
