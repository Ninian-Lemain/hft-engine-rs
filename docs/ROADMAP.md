# Roadmap

## Where the project is

Implemented through **v0.19.0**. The next committed milestone is
**v0.20.0 Fault Injection and Soak**.

Waiting on external prerequisites:

- **v0.13 Dedicated Linux Qualification** needs a dedicated Linux host
  (pinned CPU/governor/isolation) so `perf` results can be published with a
  named environment manifest. Nothing else in the roadmap depends on it;
  every later milestone's dependency chain skips over it. It stays open until
  that hardware is available.

## Scope and Rules

This is a library-first deterministic execution engine. The matching core is
single-writer and fixed-capacity. Measured hot paths perform no heap allocation
after initialization and contain no locks, blocking I/O, logging, formatting,
or syscalls.

Status is evidence-based:

- **Implemented:** code, tests, documentation, and required evidence exist.
- **Next:** the only committed implementation milestone.
- **Planned:** dependency-ordered work with no completion claim.
- **Experimental:** optional work that cannot define core APIs.
- **Post-v1 research:** hardware or venue-specific investigation.

Every release requires formatting, workspace check, Clippy with warnings
denied, tests, relevant Miri/Loom/sanitizer/property coverage, explicit capacity
and failure behavior, and a documented limitation. Performance changes require
same-workload before/after release results with workload, configuration, CPU,
OS, compiler, memory cost, and allocation delta. CI and Windows timings are not
production-latency evidence.

## Safety and Native Boundaries Before v0.3

- Public APIs, matching, risk, parsing, sessions, and replay remain safe Rust.
- Repository-owned unsafe sites require a documented invariant and test.
- Keep the audited Rust SPSC; do not move it to C++ merely to hide unsafe code.
- Use optional C++ only for a real vendor/NIC SDK behind a C ABI with opaque
  handles, fixed-width fields, explicit ownership/errors, and no exceptions or
  C++ standard-library types across the boundary.
- Default builds stay Rust-only. Native shims require ABI tests and ASan/UBSan.

This is immediate policy and verification work, not an engine feature release.
Policy status: safe-crate forbids and the unsafe allowlist are CI-enforced;
each unsafe site has a documented invariant and test ([safety](SAFETY.md));
the `vendor-sdk` C ABI tests run under ASan/UBSan in CI.

## Implemented

### v0.1.0 - Deterministic Vertical Slice

Borrowed parsing, RAII frame ownership, fixed-capacity risk/matching, bounded
SPSC handoff, owner-authorized cancel, replay digest, and portable CI. Remaining
hot-path limits were linear risk lookup, linear price discovery, FIFO shifts,
and duplicate matching traversal during preflight.

### v0.2.0 - Indexed Order Lookup

Fixed-capacity `OrderId -> resting slot` lookup with deterministic back-shift
deletion. Collision, relocation, fill, cancel, and slot-reuse invariants pass.
The documented 512-level desktop workload measured 1,329 ns to 26 ns median
cancellation latency at a 64 KiB book-size cost. See the
[performance methodology](PERFORMANCE.md) for the workload and evidence.
Per-level FIFO shifts remain.

### v0.3.0 - Stable-Slot FIFO Levels

Intrusive doubly linked per-level FIFOs with head, tail, free-list, and stable
slot handles replaced array shifts. The order index maps IDs to stable slot
handles, never FIFO positions. Handle stability across unrelated mutations,
stale-handle fail-closed behavior, disjoint live/free sets, atomic full-level
rejection, and a 600-command array-model comparison pass. The documented depth
harness measured flat 100 ns p50 head/middle/tail cancel and head fill at
depths 1 through 512 (down from 3,300/1,400/100/3,100 ns at depth 512) for a
+14% `OrderBook<1, 512>` memory cost. See the
[performance methodology](PERFORMANCE.md) for the workload and evidence.
Best-price and risk lookup stay linear.

### v0.4.0 - Indexed Risk State

Fixed-capacity `AccountId -> AccountState` and `OrderId -> Reservation` open
addressed indices with deterministic collision/back-shift deletion replaced
linear scans. Reservations live in a stable-slot free-list. Account and
reservation lookups are now O(1) at load ≤ 1/2. Handle stability across
unrelated churn, stale-handle fail-closed, disjoint live/free sets, duplicate
and full rejection, and reservation-totals-equal-exposure invariant all pass.
The documented risk harness measured up to 53x latency reduction at 90%
reservation occupancy (2,892 ns to 55 ns for cancel at occupancy 921) and
2.8x for account lookups at 90% account occupancy, with zero allocations and
unchanged logical digest. See the
[performance methodology](PERFORMANCE.md) for the workload and evidence.

### v0.5.0 - Price-Level Discovery

Sorted-level index for O(1) best-bid/ask discovery replaced O(LEVELS) linear
scan. Each side carries a sorted array of occupied level indices; insert and
remove maintain sort order. `best_crossing_level` and `simulate_sorted` now
iterate the sorted index directly, visiting only occupied levels. The sorted
index handles arbitrary price distributions. All levels, crossings, boundaries,
model comparisons, and churn invariants pass. The documented price benchmark
measured 76-86 ns mean discovery latency across sparse and dense book shapes
with zero allocations and unchanged logical digest. Best-price and risk lookup
are now O(1) with respect to level count.

### v0.6.0 - Matching Transaction Plan

A `MatchPlan` of bounded fills and resting quantity replaces the second
simulation walk: `build_plan` preflights validation, duplicates, report
capacity, level capacity, and liquidity with a single traversal of the
sorted-level index that stops at the taker's price limit; `apply_plan`
performs the mutation walk. Because preflight decides every fallible condition
against an unchanged book, plan application is infallible and there is no
rollback path. The resting path finds or allocates price levels through the
sorted-level index (binary search plus a free-slot pool); level removal from
the index is also a binary search. Capacity rejection is atomic; quantity is
conserved; each maker appears once; plan application equals the reference
matcher. A match-plan benchmark measures non-crossing, single/multi-fill,
report-full rejection, and deep rejection at 72-115 ns mean with zero
allocations and unchanged logical digest. See the
[performance methodology](PERFORMANCE.md) for the workload and evidence.

### v0.7.0 - Property and Reference Models

A shared test-support crate (`hft-model`) provides seeded deterministic
command generation, a naive array reference book, and a gateway-level
reference model that mirrors sequencing, duplicate-id policy, risk
reservations, and matching outcomes. Fixed-seed CI coverage now checks
quantity conservation, non-negative remainder, price-time priority via fill
sequence equality across multiple book shapes, owner cancel, unique terminal
transitions, reservation-equals-live-exposure account snapshots at every
step, replay digest equality over full byte streams including rejections,
and lossless SPSC delivery under seeded interleavings. The release benchmark
suite was rerun with unchanged digests and zero measured hot-path
allocations.

### v0.8.0 - Reproducible Benchmark Suite

The harness emits one machine-readable JSON record per cell (schema fixture
validated in CI against a reduced run) with p50/p90/p99/p99.9/max,
throughput, allocation deltas, deterministic checksums, and separated
component, gateway, and network boundaries. Workloads cover wire parsing,
SPSC ring traffic with occupancy and backpressure tracking, a seeded mixed
gateway session, deep-book traversals at cycled depths, head/middle/tail
FIFO operations across depths, full risk-operation sweeps at three
reservation occupancies, best-price discovery, and match-plan scenarios;
every scenario warms up untimed, validates percentile ordering, and fails on
any measured allocation. Validating the suite exposed a release-only defect:
both open-addressed indexes ran their back-shift update closures inside
debug_assert, stranding moved handles and leaking entries in release builds.
The closures now run unconditionally with regression coverage. Perf counters
(cycles, instructions, branches, cache misses) stay deferred to the dedicated
Linux qualification; Windows runs validate correctness and allocation
behavior only.

### v0.9.0 - IOC and FOK

Wire protocol v2 adds a time-in-force byte to new orders. IOC executes what
crossed at the limit and never rests; its untraded remainder is reported as
a discarded quantity and releases the risk reservation immediately, so
quantity conservation reads filled + rested + discarded. FOK preflights full
liquidity and report capacity in the plan walk and either fills completely
or rejects with `InsufficientLiquidity` before any mutation. The reference
model mirrors both semantics, seeded property sessions generate mixed TIF
traffic with per-step snapshot equivalence and replay equality, and the
benchmark suite gained IOC empty/partial/full plus FOK
reject/single/multi-fill cells alongside their GTC comparisons.

### v0.10.0 - Post-Only

Post-only orders use the best-price sorted index to reject any order that
would trade before a single mutation occurs; accepted post-only orders rest
at the level tail exactly like other makers. The reference model mirrors the
check, seeded property sessions include post-only traffic through the full
gateway equivalence, snapshot, and replay gates, and the benchmark suite
gained crossing and non-crossing post-only cells at shallow (one level) and
deep (64 level) occupancies. Explicit unit tests lock the three invariants:
never trades, joins the FIFO tail, rejection leaves book state untouched.

### v0.11.0 - Replace and Order Lifecycle

Protocol type 3 adds owned replaces. A same-price quantity reduction patches
the resting slot in place and keeps FIFO priority; a price change or increase
loses priority, re-enters at the destination tail after capacity preflight,
and rejects with `ReplaceWouldCross` when the new price would cross the
opposing best. Risk reservations adjust before the book mutates and are
restored exactly on any rejected replace; filled or canceled orders are
terminal and reject further replaces as unknown. The reference model mirrors
every transition, seeded property sessions drive replace churn through
gateway equivalence, per-step reservation-equals-exposure snapshots, and
replay equality, and explicit tests lock rejected-replace atomicity,
priority retention, and terminal immutability. Benchmarks report book-side
reduce/increase/reprice/reject cells and a separate risk-only adjustment
cell.

### v0.12.0 - Actual SPSC Loom and Unsafe Audit

The queue swaps its atomics and slot cells to Loom primitives under
`--features loom`, so CI explores the shipped algorithm directly: a
capacity-two FIFO handoff across all publication/consumption interleavings
and a capacity-one backpressure/wrap scenario asserting the rejected payload
is returned intact. The stand-in model test was removed. The unsafe audit
covers UnsafeCell sites, initialization/drop pairing, index wrap, full/empty
discrimination, and endpoint lifetimes in [SAFETY](SAFETY.md). Miri runs the
whole crate in CI with reduced iteration budgets and demonstrably rejects a
weakened Release publication as a data race. The SPSC benchmark cell is
unchanged at 43 ns mean before/after the refactor with zero allocation
deltas.

### v0.14.0 - Session State Machine

The `hft-session` crate owns the connection lifecycle (disconnected,
connecting, logon, active, recovering, logout, failed) outside the matching
core, on a deterministic virtual clock. Commands are admitted only while
Active or Recovering; gaps, duplicates, and invalid transitions fail closed
without advancing state, sequence, or timers. A heartbeat timeout drops to
Recovering with a second window armed, and a second consecutive timeout
fails the session. An exhaustive state-times-event table test pins every
cell of the transition matrix and asserts refusals leave the observable
session untouched, virtual-time tests cover both deadline paths, and a
replay fixture gates recorded gateway frames through an Active session,
disconnects mid-stream, and replays the tail without gaps after re-logon.
Benchmark cells time active admission versus duplicate rejection against
the gateway baseline. Shipped ahead of v0.13 because its dependency chain
does not include it.

### v0.15.0 - Session Recovery

A bounded retransmission buffer retains accepted command frames in order
alongside their sequences; confirmation drops the confirmed prefix, and
recovery replays exactly the retained suffix. Retention exhaustion is an
explicit error rather than silent history loss, and delayed, reordered, or
duplicated inputs are refused by sequence gating before they can duplicate a
command. Deterministic timeout tests drive the state machine into
Recovering and back: the retained window replays from the first unconfirmed
sequence, confirmation is idempotent, and benchmark cells cover active
traffic, heartbeat keep-alive, gap entry with refill bursts, and full-
retention replay.

### v0.16.0 - Bounded Command Journal

A 64-byte versioned command record carries its sequence, payload length, and
CRC32C. The matching side only builds and publishes records through the bounded
SPSC queue. The persistence worker owns storage writes, fixed-size batching,
short-write handling, and `EveryBatch` or `OnShutdown` flush policy. Storage or
validation failure poisons the worker. Recovery rejects corrupt, truncated,
duplicate, missing, unsupported, or out-of-order records. Crash fixtures scan
encoded prefixes and partial tails. Matching-side record creation, checksum,
enqueue, verification, batching, recovery, and saturation cells check allocation
deltas.

## Waiting on dedicated hardware

### v0.13.0 - Dedicated Linux Qualification

Requires a dedicated Linux host (pinned CPU, governor, isolation, IRQs) so
`perf` counters and an environment manifest can be published as raw evidence.
The scripts under `scripts/linux` capture the environment, reject unsuitable
hosts, pin CPU and NUMA placement, collect perf data, and hash raw results.
This Windows laptop and Docker Desktop do not qualify. Nothing below depends on
v0.13.

## Planned Reliability

### v0.17.0 Recovery and State Integrity

Implemented. Book, risk, and gateway state export canonical logical records.
The versioned big-endian snapshot uses SHA-256 integrity and records its
capacity shape and applied sequence. Restore rebuilds indexes and free lists,
then replays a contiguous journal tail. Compatibility fixtures fix the v1 byte
format. Tests reject corruption, truncation, unsupported versions, capacity
mismatch, noncanonical state, tail overlap, gaps, and payload sequence mismatch.

### v0.18.0 - Bounded Events

Implemented. Each sequence-valid command publishes one fixed-capacity batch
containing accepted, rejected, trade, cancel, replace, and changed top-of-book
events. Event IDs use the command sequence and batch ordinal. One SPSC slot
holds the whole command result. A full queue rejects admission before the
gateway advances its sequence or mutates state. Tests cover event order,
multi-level fills, business rejections, retry after backpressure, replay
equality, and snapshot-tail event equality.

### v0.19.0 - Multi-Instrument Routing

Implemented. A sorted fixed route table maps one instrument to each shard.
Input order does not change lookup results. The router owns one bounded command
producer and event consumer per shard. Each shard owns the matching command
consumer, event producer, gateway, risk state, and book. Unknown instruments,
invalid maps, and full command queues reject explicitly. Event pressure retains
the pending command before gateway mutation. Tests cover stable mapping, shard
isolation, command and event routing, both queue pressure paths, and invalid
configuration.

### v0.20.0 - Fault Injection and Soak

Requires v0.11/v0.14-v0.19. Hours of deterministic churn, gaps, reconnect,
queue pressure, journal stalls, snapshots, recovery, malformed input,
exhaustion, routing imbalance, and shutdown races with retained seeds. No
unexplained growth or divergence for the declared run.

The [soak runner](SOAK.md) now checks sustained routed churn, recurring queue
pressure, resumed snapshot recovery, session faults, and capacity exhaustion.
Multi-hour evidence and combined service fault coverage remain open.

## Pre-v1 Stabilization

The initial [engine facade](ENGINE.md) joins single-instrument admission,
bounded events, and journal persistence status. Tests cover queue pressure,
worker failure, shutdown, snapshot restart, and separate worker threads.
Router and session ownership, a persisted configuration manifest, operational
recovery workflows, and API review remain open.

- Add a small facade crate, validated builder, bounded command/report/event
  API, ownership/backpressure rustdoc, MSRV/features, examples, and format/API
  compatibility policy. The public API cannot bypass sequence, risk, or capacity.
- Add only the operational harness needed to validate configuration, health,
  drain, shutdown, backup/restore, upgrade, rollback, and recovery. Keep it
  outside the library core.
- Measure facade overhead against the internal gateway and rerun the dedicated
  Linux suite. No new hot allocation or unexplained regression is accepted.
- Require independent API, unsafe-boundary, recovery-format, and operations
  review before creating a v1 release candidate.

## v1.0 Entry Criteria

- Stable library API; deterministic matching/risk/sessions/events/recovery.
- Zero measured post-initialization allocation on declared hot paths.
- Explicit overload behavior for every capacity and queue.
- Dedicated-Linux raw evidence with no network-latency overclaim.
- Miri, actual-algorithm Loom coverage, sanitizers, property/fuzz, crash, and
  soak qualification.
- Install, validate, drain, shutdown, backup/restore, upgrade/rollback, health,
  and incident runbooks for any reference service.
- Explicit unsupported venue, regulatory, hardware, and deployment requirements.

## Experimental

- Tuned Linux UDP batching only after the dedicated Linux qualification; it
  stays outside the engine API.
- Real vendor/NIC SDK shims may use optional C++ under the audited C ABI policy.

## Post-v1 Research

AF_XDP, DPDK, huge pages, hardware timestamps, kernel bypass, replication,
standby promotion, and venue adapters. These require hardware and separate
failure/performance evidence.

## Non-Claims

No exchange certification, regulatory compliance, unimplemented venue
compatibility, production readiness before recovery/operations tests, or
production latency without dedicated measurement of the stated boundary.
