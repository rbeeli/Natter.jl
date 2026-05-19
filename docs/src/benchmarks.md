# Benchmarks

The comparison table below is generated from local benchmark JSON artifacts.
Run it from the repository root with:

```bash
just --justfile benchmarks/justfile with-server
```

The default run starts `nats:2.11`, runs three trials each for Natter.jl, Python
`nats.py`, Rust `nats.rs`, and Go `nats.go`, writes raw and aggregate artifacts
under `benchmarks/results/`, and updates this page and the README. Warmup work
runs before each timed region, so startup, package loading, compilation/build
time, dependency downloads, and benchmark warmup are not included in the
reported timings. The cross-client callback row uses Natter.jl's inline callback
mode; the Natter-only report includes both inline and task-backed callback rows.

<!-- NATTER_BENCHMARK_TABLE_START -->
Benchmark parameters: `50000` messages, `5000` requests, `64` byte payload, `3` trials per client, URL `nats://127.0.0.1:4222`. Timed regions exclude startup, package loading, compilation/build time, dependency downloads, and benchmark warmup.

Benchmarks use each client's common high-level publish, subscribe, request, and flush APIs. Natter.jl callback dispatch uses `callback_mode=:inline` in this comparison; the Natter-only report also includes task-backed callback rows. The table reports best-of-`3` results: rates use the highest throughput, while durations and latencies use the lowest observed value.

Optimization modes: Natter.jl runs with `julia --startup-file=no -O3 --check-bounds=no -C native`; Go runs from an explicit `go build -trimpath` binary; Rust runs from `cargo build --release`; the NATS server uses the official `nats:2.11` image.

| Metric | Natter.jl | Go nats.go | Rust nats.rs | Python nats.py |
| :--- | ---: | ---: | ---: | ---: |
| Publish batch / queued msg/s | 4,858,044 | 5,424,487 | 4,929,742 | 923,915 |
| Publish + flush each msg/s | 29,623 | 22,837 | 128,290 | 22,879 |
| Callback dispatch inline batch msg/s | 2,783,611 | 3,180,066 | 2,682,412 | 265,970 |
| Request/reply req/s | 18,868 | 11,748 | 17,372 | 9,727 |
| Request p50 latency ms | 0.046 | 0.054 | 0.049 | 0.090 |
| Request p95 latency ms | 0.115 | 0.223 | 0.116 | 0.171 |

Generated from benchmark JSON artifacts at `2026-05-19T22:31:28.363`.
<!-- NATTER_BENCHMARK_TABLE_END -->
