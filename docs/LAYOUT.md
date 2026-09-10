# Layout measurements

Measured on x86_64 Windows with Rust 1.96.0, LLVM 22.1.2, and an AMD Ryzen 7
7735HS. Release builds use fat LTO and one codegen unit.

| Type | Before bytes | After bytes |
| --- | ---: | ---: |
| Resting order slot | 64 | 56 |
| `OrderBook<1,64>` | 16,552 | 15,528 |
| `OrderBook<1,512>` | 131,240 | 123,048 |
| Risk index entry | 24 | 16 |
| `RiskEngine<64,1024>` | 93,216 | 75,808 |

Book commit `e17f78a` removes the price stored in each resting order. Export and
replace read it from the containing level. The saving is 16 bytes per configured
level/order pair across both sides.

Risk commit `c3d5efd` stores each private slot handle as `slot + 1` in a
`NonZeroUsize`. Zero represents an empty index entry. Account and order keys
keep their full ranges. Both indexes save 16 bytes per configured account or
reservation.

Wire, journal, snapshot, and public C layouts are unchanged. Model, FIFO,
replace, collision, slot reuse, and snapshot fixture tests pass.

## Desktop comparison

[Raw results](evidence/layout-2026-09-10.zip) contain five paired book runs and
ten paired risk runs. Each run contains 103 benchmark cells. Every pair has
matching checksums, allocation counts, and deallocation counts. Recovery cells
allocate on their cold paths. All measured hot paths retain zero allocations.

The book comparison uses `a4098bd` as its baseline. The risk comparison uses
`e17f78a`, which already contains the book change. Risk runs 1 through 5 execute
the baseline first. Runs 6 through 10 reverse that order.

Risk comparison values below are medians of ten per-run means, in nanoseconds.
The range covers the lowest and highest per-run mean.

| Workload | Before median | After median | Before range | After range |
| --- | ---: | ---: | --- | --- |
| Gateway rest/fill, per message | 92.5 | 103 | 81 to 118 | 74 to 152 |
| Seeded gateway mix | 112.5 | 107 | 100 to 231 | 96 to 129 |
| Route, process, retrieve event | 109.5 | 118 | 101 to 211 | 107 to 226 |

Some cells regress and others improve. Dedicated Linux measurements must
resolve the timing changes before performance qualification.
