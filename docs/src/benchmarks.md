# Benchmarks

The comparison table below is generated from local benchmark JSON artifacts.
Run it from the repository root with:

```bash
just --justfile benchmarks/justfile with-server
```

The default run starts `nats:2.11`, runs seven trials each for Natter.jl, Python
`nats.py`, Rust `nats.rs`, and Go `nats.go`, writes raw and aggregate artifacts
under `benchmarks/results/`, aggregates each metric by median, and updates this
page and the README. Warmup work runs before each timed region, GC-capable
runners collect before timing starts, and fast timed regions repeat whole rounds
until the configured minimum duration is reached, so startup, package loading,
compilation/build time, dependency downloads, and benchmark warmup are not
included in the reported timings. The cross-client callback row uses Natter.jl's
inline callback mode; the Natter-only report includes both inline and
task-backed callback rows.

The default minimum timed-region duration is five seconds. Set
`NATTER_BENCH_MIN_SECONDS` to a larger value when comparing small performance
changes or running on a noisy host.

<!-- NATTER_BENCHMARK_TABLE_START -->
Benchmark parameters: at least `200000` messages or `20000` requests per timed round, `64` byte payload, `7` trials per client, minimum timed-region duration `5.0` seconds, URL `nats://127.0.0.1:4222`. Timed regions exclude startup, package loading, compilation/build time, dependency downloads, and benchmark warmup.

Benchmarks use each client's common high-level publish, subscribe, request, and flush APIs. Fast timed regions repeat whole rounds until the minimum duration is reached, then report total operations per second. GC-capable runners collect before the timed region to avoid carrying setup and warmup garbage into the measured window. Batch publish reuses stable subject and payload values in every client: Natter.jl uses `prepare_publish`, Rust uses prebuilt `Subject` and `Bytes`, and Go/Python reuse their subject string and payload buffer. Natter.jl callback dispatch uses `callback_mode=:inline` in this comparison; the Natter-only report also includes task-backed callback rows. The table reports median-of-`7` results for each metric.

Flush semantics differ by client: Natter.jl, Go, and Python flush with a server PING/PONG round trip; Rust `async-nats` flush waits for the client writer/socket flush. Treat the Rust publish-plus-flush-each value as client-flush throughput, while request/reply rows are server round trips for all clients.

Optimization modes: Natter.jl runs with `julia --startup-file=no -O3 --check-bounds=no -C native`; Go runs from an explicit `go build -trimpath` binary; Rust runs from `cargo build --release`; the NATS server uses the official `nats:2.11` image.

| Metric | Natter.jl | Go nats.go | Rust nats.rs | Python nats.py |
| :--- | ---: | ---: | ---: | ---: |
| Publish batch buffered msg/s | 4,677,813 | 4,007,526 | 4,854,196 | 377,902 |
| Publish + flush each msg/s | 32,406 | 24,445 | 134,627 | 23,976 |
| Callback dispatch inline batch msg/s | 2,536,285 | 2,436,326 | 2,901,149 | 193,461 |
| Request/reply req/s | 15,757 | 10,167 | 9,101 | 9,694 |
| Request p50 latency ms | 0.046 | 0.057 | 0.065 | 0.092 |
| Request p95 latency ms | 0.135 | 0.292 | 0.293 | 0.161 |

Generated from benchmark JSON artifacts at `2026-05-21T00:51:20.552`.
<!-- NATTER_BENCHMARK_TABLE_END -->
