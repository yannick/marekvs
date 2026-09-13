# Diff commands measurements

Final resource-bound implementation rerun on 2026-09-13 reproduced every
recall/precision value below (7,523 ms release harness elapsed).

## B.1 candidate recall and assignment accuracy

The reproducible harness is `cargo bench -p marekvs-diff --bench recall -- --report`.
It uses seed `0x726563616c6c7632`, 1,000 accepted synthetic move+edit cases in
each 0.1 true-Jaccard bin from 0.3 through 1.0, and reports long leaves (28
tokens, word 3-grams) separately from short leaves (one token, character
4-grams). True Jaccard is computed independently from the actual shingle sets.
Candidate recall uses `NearStats::candidates_pairs`, before score thresholding
and assignment. Assignment precision and recall use the generated source leaf
to moved target leaf as ground truth. Each document also has three unrelated
source and target leaves.

Choices are the frozen defaults: theta 0.5; 32 bands by 4 rows; weights text
0.45, children 0.25, context 0.15, format 0.05, size 0.10. Generator bins that
cannot fill after 30,000 outer attempts are printed with their actual `n`.

The generator uses unique synthetic word tokens for long leaves and ASCII
single-token clauses for short leaves. It applies seeded replacement edits,
plus a seeded one-token/character append path so the 0.9 bin is reachable.
Each requested bin filled all 1,000 cases. The exact release output was:

```text
seed=0x726563616c6c7632 cases_per_bin=1000 theta=0.5 bands=32x4 weights=text:0.45,children:0.25,context:0.15,format:0.05,size:0.10
length  jaccard-bin  n     mean-j  candidate-recall  assignment-precision  assignment-recall
long    [0.3,0.4)  1000   0.333        36.40%             100.00%             36.40%
long    [0.4,0.5)  1000   0.448        74.10%             100.00%             74.10%
long    [0.5,0.6)  1000   0.551        95.90%             100.00%             95.90%
long    [0.6,0.7)  1000   0.638        99.40%             100.00%             99.40%
long    [0.7,0.8)  1000   0.781       100.00%             100.00%            100.00%
long    [0.8,0.9)  1000   0.857       100.00%             100.00%            100.00%
long    [0.9,1.0]  1000   0.961       100.00%             100.00%            100.00%
short   [0.3,0.4)  1000   0.346        35.10%             100.00%             35.10%
short   [0.4,0.5)  1000   0.445        71.60%             100.00%             71.60%
short   [0.5,0.6)  1000   0.555        94.20%             100.00%             94.20%
short   [0.6,0.7)  1000   0.635        99.70%             100.00%             99.70%
short   [0.7,0.8)  1000   0.777       100.00%             100.00%            100.00%
short   [0.8,0.9)  1000   0.865       100.00%             100.00%            100.00%
short   [0.9,1.0]  1000   0.971       100.00%             100.00%            100.00%
elapsed_ms=7943
```

The frozen default exceeds the required 85% candidate recall at the 0.5 bin:
95.90% for long leaves and 94.20% for short leaves. Assignment recall equals
candidate recall in this workload, and no incorrect I2 assignments were
emitted, giving 100% assignment precision in every bin.

## C.3b — shard capture and worker decode, 2026-09-13

Hardware: Apple M2 Max, 64 GiB RAM, Darwin arm64. Cargo `test` profile
(unoptimized + debuginfo), local temporary ondaDB directory, two shard threads,
default DIFF pool configuration. The documents were freshly inserted and read
once per size; these are warm local samples, not a sustained or cold-storage
benchmark. Concurrent development builds and tests were running on this host.

Reproduce with:

```sh
cargo test -p marekvs-engine scan_decode_measurement --lib -- --ignored --nocapture
```

Each fixture is one `doc` with 10, 100, or 1,000 `sen` children. Every sentence
contains a numbered 53–55-character clause and an attribute `role: "body"`.
No deleted anchors, expired records, or shadowed descendants are present, so
physical and live candidate record counts coincide. Scanned bytes include
internal keys, envelopes, CRDT payloads, and the head. Decoded record bytes
count path suffixes plus decoded JSON value/array payload bytes, excluding dot
and allocation overhead. Scan time is measured inside the shard closure;
decode time is measured inside the dedicated worker and includes core JSON
materialization, strict model parsing, canonical hashes, and Eid binding.
Queue waiting and admission are excluded from both stage timings.

| Children | Physical/live records | Scanned bytes | Decoded record bytes | Shard scan µs | Worker decode µs | Scan records/s | Decode scanned MiB/s |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10 | 53 | 4,434 | 1,652 | 242 | 576 | 218,557 | 7.34 |
| 100 | 503 | 42,918 | 16,502 | 1,303 | 3,056 | 385,995 | 13.39 |
| 1,000 | 5,003 | 433,122 | 165,902 | 11,452 | 31,431 | 436,853 | 13.14 |

These samples validate the stage instrumentation and indicate the allocation
and materialization cost of this small sentence-heavy workload. They do not
justify production limits, deadline settings, or tail-latency claims. Existing
limits remain provisional; cold SSTables, tombstone-heavy documents, deeper
hierarchies, large text leaves, replication contention, and release-profile
measurements need separate characterization.
