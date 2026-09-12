# Architecture

## Routed Command Path

1. A receive adapter supplies a borrowed `RxFrame`. With `FrameLease`, the
   receive buffer stays borrowed until the lease is dropped.
2. `hft-wire` validates version, type, declared and actual length, boundaries,
   endianness, and side before constructing a borrowed message.
3. `hft-router` normalizes the command and publishes it to the fixed queue for
   its configured instrument shard. Unknown instruments and full queues reject.
4. The normalized command crosses the bounded command SPSC. The matching shard
   retries a pending command before taking the next queue entry.
5. The shard checks its instrument, exact next sequence, and event capacity
   before gateway mutation. A gap or duplicate consumes no sequence. Event
   pressure retains the command for retry.
6. The gateway consumes the sequence and dispatches new, cancel, or replace.
   New orders reserve exposure before book preflight and application.
7. Execution reports are written into a caller-owned fixed report buffer. The
   gateway converts filled reservations into settled positions.
8. Owner-authorized cancellation uses a fixed-capacity `OrderId` index, removes
   the resting remainder without disturbing peer FIFO, and releases exactly
   that remaining risk reservation.
9. `hft-events` builds one fixed-capacity event batch for the command. It emits
   the terminal result and trades in execution order, then top-of-book only
   when it changed.
10. The producer publishes the complete batch in one SPSC slot. The router
    retrieves events by shard. It does not merge a global event order.

`MatchingShard` owns `BoundedEventEngine`, not `hft-engine`. This routed path
does not journal commands. Session admission is a separate caller-managed
component. Replay hashes logical state separately from command processing.

## Journaled Engine Path

`hft-engine` is a separate single-instrument entry point. It accepts a borrowed
frame or normalized command, checks lifecycle and sequence alignment, reserves
event capacity, and enqueues the journal record before gateway application.
A full event or journal queue leaves the command unconsumed.

`EngineStorage` owns both queues. The builder returns the engine, event consumer,
and journal reader. The caller assigns threads. Only `PersistenceWorker` writes
and flushes the sink. Written and durable progress are distinct. An event is
not a durability acknowledgment. Observed persistence failure closes admission,
but a command already in flight can race that failure.

After admission closes and persistence shutdown completes, the engine can
encode a snapshot at the verified journal cut. The caller drains events and
publishes a new snapshot generation. Restore rebuilds logical state, replays a
contiguous tail with the original report bound, and starts with fresh queues.
See the [workflow diagrams](../README.md#workflow-diagrams) and
[engine boundary](ENGINE.md).

## Ownership

- A frame lease keeps its RX buffer borrowed; it cannot outlive the queue borrow.
- A gateway owns one book and one corresponding risk engine.
- An order book has one writer and no shared mutable access.
- Each book owns its open-addressed order index. Index slots use deterministic
  linear probing, remain at or below 50% live load, and never grow on the heap.
- The sorted price directory is contiguous. FIFO slots keep stable handles
  when another price is inserted or removed. Each level maintains its quantity
  total, so top-of-book publication does not walk its orders.
- `SpscQueue::split` requires an exclusive queue borrow and yields exactly one
  producer and consumer.
- One event producer owns gateway admission. A full event queue is detected
  before sequence advancement or state mutation.
- The router owns command producers and event consumers indexed by shard ID.
  Each shard owns only its paired endpoints and matching gateway.
- Vendor sessions uniquely own one opaque handle and destroy it once.

## Capacity and Backpressure

- RX memory backend: `QueueError::Full`.
- SPSC: returns the original value when full.
- Event SPSC: one slot holds a complete command batch. Full means the command
  remains unconsumed and may be retried.
- Router command SPSC: a full queue rejects publication. Event backpressure
  leaves one command pending inside the target shard for retry.
- Engine journal SPSC: a full queue drops the admission token before gateway
  mutation. The caller retains the command for retry.
- Risk accounts and orders: explicit account/order capacity rejection.
- Book: explicit price-level, per-level FIFO, and report capacity rejection.
- Order index: fixed at four slots per configured per-side order capacity;
  deletions compact probe clusters in place.
- No structure silently overwrites unread or live data.

## Cold and Hot Cores

Configuration, filesystem access, formatting, logging, metrics export, socket
setup, memory registration, affinity, and shutdown coordination belong off the
hot core. The implemented gateway method contains no such work.
