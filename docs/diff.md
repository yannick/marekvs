---
title: Document comparison
description: DIFF.* — immutable document snapshots, independent edit suggestions, replicated review decisions, and two- or three-way application.
status: experimental
---

`DIFF.*` compares structured documents stored with `JSON.*`. A comparison
produces an immutable graph of suggested changes. Reviewers accept or reject
individual changes, and `DIFF.APPLY` creates an immutable result snapshot.
Working branches remain editable; application does not replace a branch.

## Choose the workflow

Use this extension for document review, version comparison, and reconciling two
sets of edits to a shared base. Inputs are canonical document trees, so a
converter must turn Word, Markdown, or another source format into the model
below. The server does not parse those file formats or provide a review UI.

| You want to… | Start with | Then |
|---|---|---|
| Compare two versions | `DIFF.COMPARE` | Review change IDs, record decisions, apply |
| Reconcile two edited branches | `DIFF.MERGE3` | Review alternatives and default accepts, apply |
| Save a version before editing | `DIFF.SNAPSHOT` | `DIFF.FORK` the snapshot into a branch |
| Upload a new full document | `DIFF.IMPORT` | Continue editing the branch or review its returned graph |

The review flow is **branch versions → immutable graph → decisions → immutable
result snapshot → optional new branch**. Only accepted suggestions contribute
to the result. IMPORT updates its destination immediately; use a separate
proposal branch when an upload needs approval before changing a working copy.

```note Experimental extension
`DIFF.*` is a marekvs extension, not a Redis command family. Suggestions and
replicated decisions are available now; production resource limits remain
provisional. The measured corpus and validation scope are linked at the end.
```

## Documents and keys

A document has a `doc` root. Containers use `c` for children; `sen` and `code`
leaves use `x` for text. Optional `a` contains scalar attributes. Supported
kinds are `doc`, `sec`, `par`, `sen`, `list`, `li`, `tbl`, `row`, `cell`, and
`code`.

```json
{"t":"doc","c":[{"t":"par","c":[{"t":"sen","x":"Payment is due in thirty days."}]}]}
```

Formatting is an optional leaf `f` array of `[start, end, attributes]` tuples.
Offsets count Unicode scalar values, with an exclusive end. Runs must be
sorted, nonoverlapping, nonempty, and adjacent equal runs must be merged.
Marks are boolean `b`, `i`, `u`, `s`, `code`, or string `link`. Sentence text
uses trimmed, single-space whitespace and explicit newline hard breaks;
code text preserves whitespace. Normalize Unicode to NFC in the converter:
the server does not validate NFC. Unknown fields and malformed shapes fail.
Integer attributes must fit signed 64-bit; encode larger identifiers as strings.

Every history uses one nonempty hash tag. All explicit and derived keys in a
DIFF call must use the same tag. Extra braces, empty names, and malformed
snapshot/graph/decision digests are rejected.

| Key | Role |
|---|---|
| `doc:{agreement}:b:main` | Mutable JSON branch; branch name is nonempty |
| `doc:{agreement}:s:<sid>` | Immutable semantic snapshot |
| `diff:{agreement}:g:<gid>` | Immutable graph, stored as a JSON string |
| `diff:{agreement}:d:<gid>` | Mutable decision document managed by `DIFF.DECIDE` |
| `diff:{agreement}:r:<token>` | Immutable application request result, stored as a JSON string |

`sid` and `gid` are 32 lowercase hexadecimal characters. Change IDs are
`c:` followed by 32 lowercase hexadecimal characters. Commands accept **full
keys**, not bare snapshot or graph digests. Request tokens are nonempty UTF-8,
at most 256 bytes, and cannot contain braces.

Snapshot identity covers visible semantic content, excluding record clocks
and source element identities. Identical content yields the same snapshot ID.
Branch comparisons can use retained element identities as correspondence
evidence; comparisons through snapshot keys do not use that evidence.

## Command reference

Map replies are RESP3 maps or alternating key/value arrays in RESP2. Fields
listed as JSON below are bulk strings containing JSON, not nested RESP values.

| Command | Reply and behavior |
|---|---|
| `DIFF.SNAPSHOT source` | Full snapshot key. Source is a branch or snapshot; captures visible content and publishes canonical immutable records. |
| `DIFF.FORK source branch [REPLACE]` | `OK`. Destination must be a distinct same-tag branch. Preserves source array identities and dead ordering anchors, refreshes destination clocks, and drops source TTL. Existing destinations require `REPLACE`. |
| `DIFF.COMPARE from to [LEVEL sen\|word\|char] [THETA number]` | `[full_graph_key, graph_JSON]`. Both inputs are branches or snapshots. Also publishes source snapshots. Default level `word`, threshold `0.5`; threshold must be finite in `[0,1]`. |
| `DIFF.HASH source [path]` | Map `{exact, content, weight}`. Hashes are 32 lowercase hex characters; weight is an integer. Path defaults to `$` and supports only child ordinals such as `$.c[0].c[2]`, not arbitrary JSONPath. |
| `DIFF.STATS` | Alternating name/value array with in-flight requests/bytes, cache bytes/hits, operation/result counters, record counters, rejection counters, and queue wait sum/count. Local to the executing process. |
| `DIFF.DECIDE graph [BY principal] id accept\|reject\|pending [id state ...]` | `OK`. Validates the entire batch, including unknown and duplicate IDs, before writing. State words are lowercase. Principal defaults to `-`; UTF-8, at most 1,024 bytes, and may contain pipes. |
| `DIFF.DECISIONS graph` | Map `{decisions, drev, unresolved}`. `decisions` is JSON keyed by change ID, with `{state, by, at}` values. States in replies are `accepted`, `rejected`, `pending`; `at` is milliseconds. `drev` is a 32-hex revision of the decision view. `unresolved` is an array of change-ID groups for the current accepted selection. |
| `DIFF.APPLY graph REQUEST token [DECISIONS revision]` | Success map `{rid, sid, unresolved: []}`; `rid` is the supplied token and `sid` the result digest. Unresolved selection returns `{unresolved: [[id, ...], ...]}` without publishing a request result. Optional revision requires the current decision view to match. |
| `DIFF.MERGE3 base left right` | `[full_graph_key, merge_JSON]`. Captures three same-tag inputs and publishes their snapshots plus a graph against the base. Uses the library defaults: word level and threshold `0.5`. |
| `DIFF.IMPORT branch tree_JSON [FROM previous] [THETA number]` | Map `{sid, gid, stats, writes}`. `gid` is a full graph key; `stats` is JSON; `writes` counts emitted branch delta records. Default threshold `0.7`. Source defaults to the destination branch; `FROM` accepts a same-tag branch or snapshot. |

`DIFF.COMPARE` returns a graph with `changes`, dependency/conflict hints, input
IDs, options, and deterministic statistics. Each change has its own ID and
operation. Moves and text edits can be separate suggestions. Use the returned
IDs; do not derive them from array indexes or assume they survive another
comparison.

`DIFF.MERGE3` returns a wrapper with `graph`, `base`, `left`, `right`,
`conflicts`, and `origins`. Origin values are `l`, `r`, or `both`. Its stored
ID seals the whole wrapper, including those inputs and review defaults;
changing only outer metadata invalidates the artifact. The embedded graph's
`to` is a synthetic merge identity, not an input snapshot to fetch.

## Review and application

For a two-way graph, missing decisions mean pending. For a three-way graph,
nonconflicting suggestions default to accepted and conflicting alternatives
default to pending. These defaults live in immutable merge metadata. Retrying
MERGE3 does not initialize mutable records over a reviewer's later decisions.
An explicit `pending` decision overrides an accepted default.

Decisions are independently replicated LWW records. Concurrent decisions for
different changes survive; concurrent decisions for the same change resolve
by the record's timestamp ordering. The stored scalar JSON tuple keeps state,
principal, and time together. The cumulative decision-document budget counts
raw CRDT bytes; a batch checks its projected merged size before any write. A decision revision includes the whole view,
including attribution and timestamps, so repeating a decision may change it.

The planner validates the accepted selection, its references, dependencies,
and final ancestry. Conflict hints alone are insufficient: accepting an escape
move can make a deletion safe, while opposing moves can create a cycle.
Resolve every returned group before expecting application to succeed.

APPLY consults an existing request result **before** reading current graphs,
snapshots, or decisions. Reusing the same token for the same graph returns
that recorded result, even after decisions change or source snapshots are
removed. Reusing a token for another graph fails with `DIFFREQUEST`. To apply
new decisions, use a new token. A fresh request captures decisions and rechecks
their revision in the publication closure; concurrent changes return
`DIFFSTALE`. The optional `DECISIONS` revision prevents applying a view older
than the one the caller reviewed.

The result record can be inspected with
`GET diff:{agreement}:r:<token>`; it includes the graph association, decision
revision, result SID, and base-reference-to-result-ordinal mapping. Read the
result with `JSON.GET doc:{agreement}:s:<sid> .`, or fork that snapshot into a
branch before further editing.

Successful COMPARE and MERGE3 publication emits a `graph` event on
`diff:{agreement}:events`; DECIDE emits a `decisions` event with the affected
IDs. Subscribe with ordinary `SUBSCRIBE`. Events are notifications, not a
durable review log: fetch the graph or decision view to recover after a
subscriber reconnects.

## Import and branch editing

Use `JSON.*` for ordinary branch edits. IMPORT takes a complete canonical tree
and computes changes against the previous version while preserving identities
where matching supports them. Unchanged records remain untouched; moving a
container rewrites its descendant paths under the new location. Consequently,
a moved subtree's delta size grows with its record count.

A distinct `FROM` source fully initializes or replaces the destination; it is
not a patch that assumes the destination already contains the source. IMPORT
rechecks both captured source and destination state before publication and
returns `DIFFSTALE` if either changed. Retry after reading the new state.
Repeated equal imports can produce zero branch delta writes. The first raw
upload has no carried identity evidence; later branch comparisons can use the
identities retained by import.

## Try a complete review

Start a local server using the [Quickstart](../quickstart/). This Bash example
requires `redis-cli` with RESP3/JSON output and `jq`. It uses disposable
`{diff-demo}` branches; choose a different tag if those names hold useful data.
Set `DIFF_DEMO_PORT` if your server uses a port other than 6379.

```bash
set -euo pipefail
r() { redis-cli -h 127.0.0.1 -p "${DIFF_DEMO_PORT:-6379}" -3 --json "$@"; }

r JSON.SET 'doc:{diff-demo}:b:base' '$' \
  '{"t":"doc","c":[{"t":"sen","x":"Pay in thirty days."}]}'
r DIFF.FORK 'doc:{diff-demo}:b:base' 'doc:{diff-demo}:b:proposal' REPLACE
r JSON.SET 'doc:{diff-demo}:b:proposal' '$.c[0].x' '"Pay in sixty days."'

comparison=$(r DIFF.COMPARE 'doc:{diff-demo}:b:base' 'doc:{diff-demo}:b:proposal')
graph=$(jq -er '.[0]' <<< "$comparison")
# Graph JSON is itself a string inside the RESP array. Inspect before accepting.
jq '.[1] | fromjson | .changes' <<< "$comparison"
change=$(jq -er '.[1] | fromjson | .changes[0].id' <<< "$comparison")
r DIFF.DECIDE "$graph" BY reviewer "$change" accept

view=$(r DIFF.DECISIONS "$graph")
revision=$(jq -er '.drev' <<< "$view")
# A fresh token starts a new application; keep this token for exact retries.
request="review-$(date +%s)-$$-$RANDOM"
result=$(r DIFF.APPLY "$graph" REQUEST "$request" DECISIONS "$revision")
sid=$(jq -er '.sid' <<< "$result")
r JSON.GET "doc:{diff-demo}:s:$sid" '.' | jq -r 'fromjson | .c[0].x'
```

The final line prints `Pay in sixty days.` The base branch still contains
`Pay in thirty days.` Rejecting or leaving the suggestion pending would retain
the base text. This fixture has one change; in a real graph, inspect and choose
each ID rather than accepting the first entry automatically.

To continue editing the approved result, use the same shell variables:

```bash
r DIFF.FORK "doc:{diff-demo}:s:$sid" 'doc:{diff-demo}:b:approved' REPLACE
```

### Review a three-way merge

Create left and right branches from the same base with `DIFF.FORK`, edit each,
then call `DIFF.MERGE3 base left right` with their full keys. Keep all three
under one tag. Read the returned wrapper's `graph.changes` and `origins` to
show who proposed each suggestion, then call `DIFF.DECISIONS` for the effective
selection.

| Situation | Initial decision | Reviewer action |
|---|---|---|
| Independent suggestions | Accepted | Keep or explicitly reject/pause them |
| Competing alternatives | Pending | Accept the intended alternative; reject the other |
| Current selection has a structural conflict | Returned in `unresolved` | Adjust its decisions and inspect again |

Pending alternatives are omitted from the result; APPLY can succeed while
some changes remain pending. An empty `unresolved` list means the **accepted
selection is valid**, not that every suggestion has been reviewed. A UI that
requires a complete review should additionally check for pending decisions.

### Retry without applying a different review

| Response or situation | Next step |
|---|---|
| Connection lost during APPLY | Resend the same graph, token, and revision to retrieve any recorded result |
| `DIFFSTALE` for a fresh application | Fetch decisions again, review them, and submit their revision |
| `{unresolved: [...]}` | Adjust the selected changes; no request result was stored |
| Successful application, then more decisions | Use a new request token to apply the new view |
| `DIFFREQUEST` | The token belongs to another graph; choose a new token |

Retain the returned snapshot key in your application. Reading a stored request
can return its original result even if an operator has since removed that
snapshot; retrying is not a snapshot recovery operation.

## Limits, immutability, and consistency

Admission precedes DIFF storage reads. The dedicated worker count defaults to
`max(1, min(8, floor(available CPUs / 4)))`. Each input is bounded by 16 MiB of
scanned record bytes, 50,000 live candidate records, and 200,000 physical
records. These counts differ from the semantic tree's node limit. Dead anchors
and invisible records still consume physical scan capacity.

Total in-flight **input reservations** default to
`3 × MAX_BYTES × CONCURRENCY`, which admits a three-input request. Tree,
graph, candidate, edit-work, and cache allocations have separate bounds;
the input byte gauge is not a process RSS ceiling. The queue holds at most
one job per worker, with at most twice the worker count in admitted requests.
Admission and queue saturation return `DIFFBUSY` immediately. A queued job has
a 2-second queue deadline and a request has a 5-second deadline from admission.
Shard operations remain synchronous: deadlines are checked before publication
and at worker/queue boundaries, so they are not a hard interruption of an
in-progress storage closure. Graph deserialization and digest verification are
currently bounded shard work; semantic comparison and planning use the pool.

Dropped callers cancel queued/running work. Worker and shard closures retain
the admission reservation until their allocations finish, so a timeout does
not immediately make the same capacity available to another caller. The
semantic-only LRU cache defaults to 256 MiB and never carries branch bindings.
All environment settings and aliases are listed in the
[authoritative defaults table](https://github.com/yannick/marekvs/blob/main/design/05-consistency-anti-entropy.md#defaults-table).

Ordinary client mutation commands cannot modify snapshot, graph, or result
prefixes, even when the suffix is malformed. The guard covers JSON/string
writers, collection destinations, multi-key operations, and Lua calls, and
validates all affected keys before writes. Read-only sources such as COPY's
source are permitted. Ordinary reads remain available. `FLUSHDB`/`FLUSHALL`
are explicit exemptions; internal replication and upstream replication apply
also bypass the convention. This is not an authorization boundary.

All ten DIFF commands are rejected inside Lua scripts. They remain outside
the parallel pipeline whitelist; clients may pipeline them with normal ordered
dispatch. Seven mutating verbs obey disk/compaction write stops; HASH, STATS,
and DECISIONS are read-only.

A snapshot describes the executing node's observed state. Same-tag inputs are
captured in one shard closure, but this does not establish cluster-wide
freshness or global transactions. Decisions and artifacts converge through
normal replication. After partitions, wait for the desired decision revision
before applying. Local request-token reuse is checked atomically; simultaneous
first use of the same token on disconnected nodes is not a global lock.

## Errors

| Error | Meaning / response |
|---|---|
| `DIFFKEY`, `CROSSSLOT` | Malformed key, wrong key role, or different tags. Correct the request. |
| `DIFFIMMUTABLE` | An ordinary client writer targeted a reserved artifact. Fork a snapshot into a branch. |
| `DIFFMODEL` | Tree shape, attributes, whitespace, formatting, or model limits are invalid. |
| `DIFFTOOBIG` | Input, graph, record, work, or output bound exceeded. Reduce the request or adjust validated limits. |
| `DIFFBUSY` | Request, input-byte, or queue capacity exhausted. Retry with backoff. |
| `DIFFTIMEOUT`, `DIFFCANCELLED` | Queue/request deadline or cancellation. Retry only if still needed. Capacity remains reserved until work finishes. |
| `DIFFNOSNAPSHOT`, `DIFFNOGRAPH` | Required artifact/input is missing or incomplete. Allow replication/retry publication. |
| `DIFFEXISTS` | FORK destination exists without `REPLACE`. |
| `DIFFSTALE` | Captured source/destination/decision state changed. Read again and retry; preserve an existing APPLY token for exact retries. |
| `DIFFCHANGE`, `DIFFPRINCIPAL`, `DIFFDECISIONS` | Invalid decision ID/batch/principal or malformed stored decision data. |
| `DIFFREQUEST` | Invalid token or token already associated with another graph. |
| `DIFFCOLLISION`, `DIFFINVALID` | Artifact contents/digest disagree, or a graph/path is invalid. Inspect the data before retrying. |
| `DIFFWRITE`, `DIFFSTORAGE`, `DIFFWORKER`, `DIFFCONFIG` | Storage publication, worker execution, or configuration failed. Inspect server diagnostics. |
| `WRONGTYPE`, `ERR`, `MISCONF` | Standard type, syntax/storage, or write-stop error. |

Unresolved selections are structured APPLY replies, not Redis errors.

## Observability and validation

Prometheus exposes `marekvs_diff_operations_total{op,result}`,
`marekvs_diff_stage_seconds{stage}`, `marekvs_diff_records_total{kind}`,
`marekvs_diff_inflight_bytes`, `marekvs_diff_inflight_requests`, cache bytes/hits,
queue wait, and rejection counters. `DIFF.STATS` returns local counters without
requiring the metrics endpoint.

The [design](https://github.com/yannick/marekvs/blob/main/design/19-diff.md) explains matching and publication. The
[measurements](https://github.com/yannick/marekvs/blob/main/docs/superpowers/plans/2026-09-13-diff-commands-measurements.md)
record synthetic correspondence recall and warm scan/decode samples. Limits
remain provisional: those samples do not establish cold-storage or production
tail latency. The partition negotiation scenario is
`just chaos diff_negotiation`; run it with the intended Docker/runtime setup
before relying on that environment's partition behavior.
