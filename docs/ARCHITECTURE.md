# Architecture

## Data Path

1. An RX queue grants a frame lease borrowing driver-owned or preallocated
   memory. Dropping the lease permits descriptor/buffer recycling.
2. `hft-wire` validates version, type, declared and actual length, boundaries,
   endianness, and side before constructing a borrowed message.
3. `hft-router` normalizes the command and publishes it to the fixed queue for
   its configured instrument shard. Unknown instruments and full queues reject.
4. The gateway requires the exact next per-instrument sequence before state
   mutation. A gap or duplicate fails closed and does not advance the expected
   sequence.
5. The normalized command crosses cores in a preallocated SPSC slot.
6. A matching shard checks and reserves account exposure, then mutates its
   single-writer book.
7. Execution reports are written into a caller-owned fixed report buffer. The
   gateway converts filled reservations into settled positions.
8. Owner-authorized cancellation uses a fixed-capacity `OrderId` index, removes
   the resting remainder without disturbing peer FIFO, and releases exactly
   that remaining risk reservation.
9. `hft-events` builds one fixed-capacity event batch for the command. It emits
   the terminal result, trades in execution order, and the final top-of-book
   change.
10. The producer publishes the complete batch in one SPSC slot. Replay hashes
   stable logical state, independent of array slot placement.

## Ownership

- A frame lease keeps its RX buffer borrowed; it cannot outlive the queue borrow.
- A gateway owns one initially empty book and one corresponding risk engine.
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
- Risk accounts and orders: explicit account/order capacity rejection.
- Book: explicit price-level, per-level FIFO, and report capacity rejection.
- Order index: fixed at four slots per configured per-side order capacity;
  deletions compact probe clusters in place.
- No structure silently overwrites unread or live data.

## Cold and Hot Cores

Configuration, filesystem access, formatting, logging, metrics export, socket
setup, memory registration, affinity, and shutdown coordination belong off the
hot core. The implemented gateway method contains no such work.
