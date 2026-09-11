# Fault and soak runs

`hft-soak` runs each seed twice and compares state fingerprints, event
fingerprints, and scenario counters. Failures print the seed, phase, error,
and replay command. Root seeds are retained in `crates/hft-soak/seeds/v1.txt`.

| Scenario | Work per pass |
| --- | --- |
| Routed churn | Declared steps across four shards with 85/10/4/1 routing weights |
| Session faults | One reconnect, gap, duplicate, timeout, and retransmit cycle per 64 steps, rounded up |
| Recovery | Declared commands, with snapshot and tail recovery every 64 commands |
| Journal faults | One saturation, short-write, shutdown, crash-cut, and poisoned-worker fixture |
| Capacity | One set of account, order, level, report, retransmit, and queue exhaustion fixtures |

Routed commands target live orders for cancels and replaces. Command and event
pressure recur every 256 commands when at least nine commands remain. Each
processed command must produce one terminal event. Checks compare event payloads
with the live order table and verify that other shards did not change.

Recovery installs the restored gateway at each checkpoint. Subsequent outcomes
and reports must match an uninterrupted gateway. Full logical state must match
at each checkpoint. The retained journal tail is bounded at 4,096 bytes.

Journal faults use an in-memory sink. They do not exercise a real disk or share
the routed command stream. Concurrent producer-close races have separate tests
in `hft-journal`. The soak runs do not yet qualify a complete service shutdown.

## Run

```text
cargo run --release -p hft-soak -- --profile smoke
cargo run --release -p hft-soak -- --seed 0000000000000001 --steps 1000000
cargo run --release -p hft-soak -- --profile nightly --seed-file crates/hft-soak/seeds/v1.txt
```

Profiles select fixed step counts. Smoke uses 10,000, nightly uses 10,000,000,
and qualification uses 100,000,000. `--steps` overrides the count. A profile
name does not prove elapsed hours or dedicated Linux qualification.

The JSON result leaves `peak_rss_bytes` null when memory was not measured.
On Windows, capture external process samples and run metadata with:

```powershell
cargo build --release -p hft-soak
./scripts/soak/run_windows.ps1 -OutputDirectory target/soak-run -Seed 0000000000000001 -Steps 100000000
```

The output directory must not exist. It receives the result, stderr, source
hashes, binary hash, environment, elapsed time, and one-second memory samples.
Working set measures resident memory. Private bytes measure committed private
memory. These measurements include the harness and allocator. They are not
hot-path allocation counts or latency measurements.

v0.20 remains open until multi-hour evidence and the remaining combined fault
coverage are recorded. Dedicated Linux performance qualification is separate.
