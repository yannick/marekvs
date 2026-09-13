//! Deterministic candidate-recall and assignment-accuracy measurement.
use marekvs_diff::{
    matching::{Layer, Matching},
    near, Options, Tree,
};
use serde_json::json;
use std::{collections::BTreeSet, time::Instant};

const SEED: u64 = 0x7265_6361_6c6c_7632;
const PER_BIN: usize = 1_000;
const BINS: &[(f64, f64)] = &[
    (0.3, 0.4),
    (0.4, 0.5),
    (0.5, 0.6),
    (0.6, 0.7),
    (0.7, 0.8),
    (0.8, 0.9),
    (0.9, 1.000_000_1),
];

#[derive(Clone)]
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    fn usize(&mut self, n: usize) -> usize {
        (self.next() as usize) % n
    }
}

fn jaccard(a: &str, b: &str) -> f64 {
    // Deliberately independent of `near::shingles`: retain the source material
    // itself rather than comparing the matcher's hashes.
    fn shingles(text: &str) -> BTreeSet<Vec<u8>> {
        let words = text.split_whitespace().collect::<Vec<_>>();
        if text.is_empty() {
            return BTreeSet::new();
        }
        if words.len() > 16 {
            words
                .windows(3)
                .map(|w| {
                    let mut key = vec![b'w'];
                    for s in w {
                        key.extend_from_slice(&(s.len() as u64).to_be_bytes());
                        key.extend_from_slice(s.as_bytes());
                    }
                    key
                })
                .collect()
        } else {
            let chars = text.chars().collect::<Vec<_>>();
            if chars.len() < 4 {
                BTreeSet::from([[vec![b'c'], text.as_bytes().to_vec()].concat()])
            } else {
                chars
                    .windows(4)
                    .map(|w| {
                        let mut key = vec![b'c'];
                        key.extend(w.iter().collect::<String>().as_bytes());
                        key
                    })
                    .collect()
            }
        }
    }
    let a = shingles(a);
    let b = shingles(b);
    let union = a.union(&b).count();
    if union == 0 {
        0.0
    } else {
        a.intersection(&b).count() as f64 / union as f64
    }
}

fn long_base(case: usize) -> String {
    (0..28)
        .map(|i| format!("term_{case}_{i}"))
        .collect::<Vec<_>>()
        .join(" ")
}
fn long_edit(base: &str, rng: &mut Rng) -> String {
    let mut w = base
        .split_whitespace()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if rng.usize(12) == 0 {
        w.push(format!("append_{}", rng.next()));
        return w.join(" ");
    }
    let changes = 1 + rng.usize(17);
    for k in 0..changes {
        let i = rng.usize(w.len());
        w[i] = format!("revision_{}_{}", rng.next(), k);
    }
    w.join(" ")
}
fn short_base(case: usize) -> String {
    format!("clause{case:08x}abcdefghijklmnopqrstuvwx")
}
fn short_edit(base: &str, rng: &mut Rng) -> String {
    let mut c = base.chars().collect::<Vec<_>>();
    if rng.usize(12) == 0 {
        c.push((b'A' + rng.usize(26) as u8) as char);
        return c.into_iter().collect();
    }
    let changes = 1 + rng.usize(18);
    for _ in 0..changes {
        let i = rng.usize(c.len());
        c[i] = (b'A' + rng.usize(26) as u8) as char;
    }
    c.into_iter().collect()
}

fn case_text(
    kind: &str,
    serial: usize,
    lo: f64,
    hi: f64,
    rng: &mut Rng,
) -> Option<(String, String)> {
    let a = if kind == "long" {
        long_base(serial)
    } else {
        short_base(serial)
    };
    for _ in 0..2_000 {
        let b = if kind == "long" {
            long_edit(&a, rng)
        } else {
            short_edit(&a, rng)
        };
        let j = jaccard(&a, &b);
        if j >= lo && j < hi && a != b {
            return Some((a, b));
        }
    }
    None
}

fn tree(texts: &[String]) -> Tree {
    Tree::from_json(
        &json!({"t":"doc","c":texts.iter().map(|x|json!({"t":"sen","x":x})).collect::<Vec<_>>() }),
    )
    .unwrap()
}

fn measure(
    kind: &str,
    lo: f64,
    hi: f64,
    rng: &mut Rng,
    options: &Options,
    serial: &mut usize,
) -> (usize, usize, usize, usize, usize, f64) {
    let mut generated = 0;
    let mut candidate_tp = 0;
    let mut assignment_tp = 0;
    let mut assignment_fp = 0;
    let mut attempts = 0;
    let mut sum_j = 0.0;
    while generated < PER_BIN && attempts < PER_BIN * 30 {
        attempts += 1;
        *serial += 1;
        let Some((a_text, b_text)) = case_text(kind, *serial, lo, hi, rng) else {
            continue;
        };
        let distract_a = (0..3)
            .map(|i| {
                format!(
                    "unrelated source {kind} {} {i} alpha beta gamma delta",
                    *serial
                )
            })
            .collect::<Vec<_>>();
        let distract_b = (0..3)
            .map(|i| {
                format!(
                    "foreign target {kind} {} {i} quartz ivory ember zinc",
                    *serial
                )
            })
            .collect::<Vec<_>>();
        let mut aa = vec![a_text.clone()];
        aa.extend(distract_a);
        let mut bb = distract_b;
        bb.push(b_text.clone()); // ground truth moves leaf 1 -> leaf 4
        let (a, b) = (tree(&aa), tree(&bb));
        let mut matching = Matching::new(&a, &b);
        matching.set(0, 0, Layer::I3);
        let stats = near::run(&a, &b, &mut matching, options).unwrap();
        if stats.candidates_pairs.contains(&(1, 4)) {
            candidate_tp += 1;
        }
        let correct = matching.a2b(1) == Some(4);
        assignment_tp += usize::from(correct);
        assignment_fp += matching
            .pairs()
            .filter(|&(x, y)| matching.layer(x) == Some(Layer::I2) && (x, y) != (1, 4))
            .count();
        sum_j += jaccard(&a_text, &b_text);
        generated += 1;
    }
    (
        generated,
        candidate_tp,
        assignment_tp,
        assignment_fp,
        generated - assignment_tp,
        sum_j,
    )
}

fn pct(n: usize, d: usize) -> f64 {
    if d == 0 {
        0.0
    } else {
        100.0 * n as f64 / d as f64
    }
}
fn main() {
    let report = std::env::args().any(|x| x == "--report");
    let options = Options::default();
    let start = Instant::now();
    let mut rng = Rng(SEED);
    let mut serial = 0;
    println!("seed=0x{SEED:016x} cases_per_bin={PER_BIN} theta={} bands={}x{} weights=text:{:.2},children:{:.2},context:{:.2},format:{:.2},size:{:.2}",options.theta,options.bands.0,options.bands.1,options.weights.text,options.weights.children,options.weights.context,options.weights.format,options.weights.size);
    println!("length  jaccard-bin  n     mean-j  candidate-recall  assignment-precision  assignment-recall");
    for kind in ["long", "short"] {
        for &(lo, hi) in BINS {
            let (n, ctp, atp, afp, afn, sum) =
                measure(kind, lo, hi, &mut rng, &options, &mut serial);
            let end = if hi > 1.0 { "]" } else { ")" };
            println!("{kind:5}   [{lo:.1},{:.1}{end}  {n:4}  {:6.3}       {:6.2}%             {:6.2}%            {:6.2}%",hi.min(1.0),if n==0{0.0}else{sum/n as f64},pct(ctp,n),pct(atp,atp+afp),pct(atp,atp+afn));
        }
    }
    if report {
        println!("elapsed_ms={}", start.elapsed().as_millis());
    }
}
