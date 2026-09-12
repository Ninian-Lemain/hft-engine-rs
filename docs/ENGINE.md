# Engine Boundary

`hft-engine` owns one instrument's gateway, event producer, and journal writer.
Its public API exposes no mutable gateway or journal writer. This is the first
pre-v1 facade. It does not yet own the multi-instrument router or sessions.

## Construction and Admission

`EngineBuilder` checks engine capacities, account registration, and risk limits.
`EngineStorage` checks the event queue capacity. Building checks the event batch
bound and attaches both queues at the gateway's next sequence. A successful
build uses that storage for one engine lifetime. Restart needs fresh storage.

`EngineParts` separates the engine, event consumer, and journal reader. Move
the consumers to their workers before admission. No storage calls run inside
`process_command` or `process_frame`. The frame path parses once and journals
the original bytes. The command path encodes once after admission checks.

Both paths reserve event capacity, enqueue the command, then apply it. A full
event or journal queue consumes nothing. Retain the refused command and retry
it unchanged. Sequence and instrument refusals also consume nothing.
Sequence-valid business rejections consume the sequence and publish a rejection
event. A successful method return means the command was processed, not that
the order was accepted or the journal was flushed.

Observed persistence failure closes admission. A failure can race a command
already in flight. Dropping the persistence reader before clean shutdown also
poisons the engine. Internal apply failures and sequence exhaustion stop it.
There is no in-place resume after failure.

## Shutdown and Recovery

1. Stop upstream admission and decide whether to drain or refuse queued inputs.
2. Call `stop_admission` after the last command. Dropping the engine also closes
   its journal producer, but loses access to its state snapshot.
3. Finish `PersistenceWorker::shutdown`. It drains the journal and flushes the
   sink. `health` reports written and durable progress separately.
4. Drain the event consumer before retiring queue storage. Event delivery has
   no durable acknowledgment or outbox in this API.
5. Call `snapshot` after successful persistence shutdown. It checks that both
   journal watermarks match the gateway cut. Encoding allocates on this cold
   path. Publish with `persist_snapshot_new` to a new generation path.
6. Restore an authoritative snapshot and contiguous journal tail with
   `EngineBuilder::restore`. Start the resulting engine with fresh storage.

Restore checks the snapshot's gateway capacity shape and expected instrument.
Snapshot v1 does not record `REPORTS`. Use the original report bound during
tail replay. A persisted configuration manifest and mismatch rejection remain
open work. Historical events are not republished by this restore method.

The example writes one command, closes admission, flushes a file, and publishes
a snapshot. It requires a directory that does not exist yet:

```text
cargo run --release -p hft-engine --example lifecycle -- target/engine-example
```

The example drives persistence on the caller thread after admission stops.
The lifecycle tests also run admission, persistence, and event consumption on
three threads and check the final command after producer closure.

## Compatibility and Remaining Work

The crate uses workspace version 0.19.0 and MSRV 1.85. It has no crate-specific
features or new third-party dependencies. Its Rust API remains pre-v1.
Wire records, 64-byte journal records, and snapshot v1 bytes are unchanged.
In-memory Rust layouts are not serialization formats.

Router ownership, session admission, configuration manifests, snapshot
selection and retention, upgrade and rollback checks, combined fault soak,
and independent API review remain open. The desktop
[overhead measurements](PERFORMANCE.md#engine-boundary) do not qualify Linux
production latency.
