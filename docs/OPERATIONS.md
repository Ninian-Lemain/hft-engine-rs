# Operational Model

## Process Layout

The intended Linux deployment uses one single-writer shard per instrument.
Cold-path control configures sockets, memory, rings, affinity,
NUMA placement, logging, metrics export, and shutdown before the hot loop.
Inbound work crosses cores only as a normalized fixed-size SPSC value.

## Fail-Closed Conditions

- Invalid packet/version/type/length/side: parse rejection.
- Duplicate or missing sequence: rejection without advancing expected sequence.
- Risk limit, kill switch, account, or order capacity failure: order rejection.
- Report, level, or per-price FIFO exhaustion: book rejection after preflight.
  The gateway releases the taker reservation.
- Unauthorized or unknown cancel: rejection without changing book or risk state.
- SPSC full: producer retains the value and observes explicit backpressure.
- RX ring full: backend rejects rather than overwriting an unread frame.
- Event ring full: the command is not admitted, its sequence is not consumed,
  and gateway state is unchanged.

Sequence-valid business rejections consume the command sequence. Rejected new
orders can also advance order ID watermarks. Resting orders and account
exposure remain unchanged. `RiskState` errors stop processing. They include
sequence exhaustion and inconsistent internal risk state.

## Event Boundary

One queue entry contains every event produced by one command. Consumers never
observe a partial command result. Event order within a batch is terminal event,
trade events in book execution order, then changed top-of-book. A
sequence-valid business rejection emits one rejection event. Parse and sequence
errors emit no event.

The caller must retain a backpressured frame and retry it after the consumer
reclaims queue capacity. Treat `EventCapacityInvariant` and
`PublicationInvariant` as fatal engine defects. They can occur only if the
configured batch bound or the single-producer capacity contract is broken.

For joint journal and event admission, verify the journal sequence, obtain an
`admit` token, enqueue the encoded command, then call `apply`. Journal pressure
drops the token without changing gateway state. An error after enqueue must
stop admission. The integration tests cover both pressure paths and retry.

## Journal Progress

A controlled `JournalChannel` exposes a `JournalStatusReader`. Written progress
advances only after a whole batch is written. Durable progress advances only
after a successful sink flush. Dequeue alone advances neither watermark.
A partial write failure leaves the previous complete-batch watermark intact.

Both watermarks identify the first sequence not covered. They start at the
sequence supplied to `split`. An event is not a durability acknowledgment.
Raw SPSC constructors provide no shared persistence status.

Worker failure or abandonment poisons the status. Taking its sink before
shutdown also poisons it. Callers must stop admission on poison, although a
command already in flight can race a storage failure. Producer closure, queue
drain, and final flush must finish before `shutdown_complete` becomes true.
Repeated successful shutdown does not flush again.

## Recovery Boundary

`hft-recovery` restores a versioned snapshot and a contiguous journal tail.
The snapshot stores logical gateway, risk, and book state with its capacity
shape, applied sequence, and SHA-256 digest. Restore rejects corrupt,
truncated, unsupported, noncanonical, or capacity-incompatible state. Tail
replay rejects overlap, gaps, partial records, corrupt records, and a mismatch
between the journal sequence and wire payload sequence.

Snapshot publication accepts only a new generation path. It syncs the file and,
on Unix, its parent directory. The API distinguishes failure before publication
from failure after the destination became visible. The repository does not yet
provide generation naming, manifest replacement, snapshot retention, or
automatic selection of the latest valid snapshot. An adapter must stop order
admission, select an authoritative snapshot and tail, restore them, verify the
result, and only then reopen the shard.

## Linux Qualification Checklist

- Pin shard and NIC queue IRQs to topology-aware isolated cores.
- Place UMEM, book, risk, SPSC, and TX frames on the NIC-local NUMA node.
- Prefault and lock memory; validate hugepage policy outside the hot loop.
- Exercise RX/TX exhaustion, link reset, process shutdown, and recovery.
- Record kernel, firmware, mitigations, governor, compiler flags, offered load,
  percentiles, queue occupancy, cache/branch misses, context switches, page
  faults, and allocation deltas.
- Never promote hosted-runner or Windows timing to a latency SLO.

## Integration Boundaries

- `UdpRx` is a portable syscall baseline.
- `af-xdp` is a truthful feature-gated marker until real descriptor/UMEM
  ownership is implemented on Linux.
- `VendorSession` is a safe ownership wrapper around an unavailable SDK, not a
  simulated vendor backend.
