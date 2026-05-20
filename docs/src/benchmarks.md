# Benchmarks

The comparison table below is generated from local benchmark JSON artifacts.
Run it from the repository root with:

```bash
just --justfile benchmarks/justfile with-server
```

The default run starts `nats:2.11`, runs five trials each for Natter.jl, Python
`nats.py`, Rust `nats.rs`, and Go `nats.go`, writes raw and aggregate artifacts
under `benchmarks/results/`, aggregates each metric by median, and updates this page and the README. Warmup work
runs before each timed region, so startup, package loading, compilation/build
time, dependency downloads, and benchmark warmup are not included in the
reported timings. The cross-client callback row uses Natter.jl's inline callback
mode; the Natter-only report includes both inline and task-backed callback rows.

<!-- NATTER_BENCHMARK_TABLE_START -->
Benchmark parameters: `200000` messages, `20000` requests, `64` byte payload, `7` trials per client, URL `nats://127.0.0.1:4222`. Timed regions exclude startup, package loading, compilation/build time, dependency downloads, and benchmark warmup.

Benchmarks use each client's common high-level publish, subscribe, request, and flush APIs. Batch publish reuses stable subject and payload values in every client: Natter.jl uses `prepare_publish`, Rust uses prebuilt `Subject` and `Bytes`, and Go/Python reuse their subject string and payload buffer. Natter.jl callback dispatch uses `callback_mode=:inline` in this comparison; the Natter-only report also includes task-backed callback rows. The table reports median-of-`7` results for each metric.

Flush semantics differ by client: Natter.jl, Go, and Python flush with a server PING/PONG round trip; Rust `async-nats` flush waits for the client writer/socket flush. Treat the Rust publish-plus-flush-each value as client-flush throughput, while request/reply rows are server round trips for all clients.

Optimization modes: Natter.jl runs with `julia --startup-file=no -O3 --check-bounds=no -C native`; Go runs from an explicit `go build -trimpath` binary; Rust runs from `cargo build --release`; the NATS server uses the official `nats:2.11` image.

| Metric | Natter.jl | Go nats.go | Rust nats.rs | Python nats.py |
| :--- | ---: | ---: | ---: | ---: |
| Publish batch buffered msg/s | 4,417,106 | 4,926,895 | 4,766,981 | 687,192 |
| Publish + flush each msg/s | 29,239 | 35,642 | 116,210 | 21,555 |
| Callback dispatch inline batch msg/s | 2,615,856 | 3,015,943 | 2,601,885 | 179,623 |
| Request/reply req/s | 17,188 | 16,917 | 16,583 | 9,328 |
| Request p50 latency ms | 0.046 | 0.046 | 0.049 | 0.091 |
| Request p95 latency ms | 0.117 | 0.118 | 0.117 | 0.172 |

Generated from benchmark JSON artifacts at `2026-05-20T12:17:46.968`.
<!-- NATTER_BENCHMARK_TABLE_END -->
