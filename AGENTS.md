# Agent Guidance For Natter.jl

Natter.jl is a pure Julia NATS client implementation.

## Source Layout

- `src/Natter.jl`: module entrypoint, imports, exports, constants, and includes only.
- `src/errors.jl`: exception types and `showerror` methods.
- `src/tasks.jl`: private Julia task spawning helpers used by background client work.
- `src/types/`: shared public and internal data types, grouped by common utilities, headers, messages, options, protocol/transport support, and client state.
- `src/protocol/`: NATS wire parsing, header parsing/validation, command serialization, subject validation, and byte conversion.
- `src/connection/`: connect setup, TLS/socket transport handling, reader/ping/session lifecycle, reconnect, discovered servers, and pending replay.
- `src/core/`: publish, subscribe, request/reply, flush, drain, close, slow-consumer dispatch, and inbox generation.
- `src/jetstream/`: JetStream types, context/base helpers, publish futures, management APIs, pull protocol/subscriptions/streams, push runtime/API, message metadata, and ack verbs.
- `src/keyvalue.jl`: KeyValue bucket and key operations built on JetStream.
- `docs/`: DocumenterVitepress documentation subproject. Source pages live in `docs/src`; the docs entrypoint is `docs/make.jl`.

Do not put substantial implementation back into `src/Natter.jl`. New features should go into the closest existing file or a new focused file included from `src/Natter.jl`.

## Compatibility And API

- Julia target is `1.10+`.
- Backward compatibility is not a constraint yet; prefer correctness and maintainability.
- APIs should be ergonomic and Julia-idiomatic: prefer clear method signatures, keyword arguments for optional behavior, concrete return types where practical, and Base extensions only when they fit established Julia conventions.
- Public APIs should remain directly task-friendly. Prefer direct Natter calls composed with Julia `Task`s for background or fan-out work instead of adding public `_async` wrapper APIs.
- Internal background work must go through the shared helpers in `src/tasks.jl`; do not add one-off raw `@async` wrappers.
- When adding or changing a public blocking operation, update exports, docs, and tests for both sync and async forms unless there is a clear reason not to expose an async twin.
- Internals use Julia tasks for reader, ping, reconnect, callback processing, and public async handles.
- Documentation should teach idiomatic Julia task concurrency first: direct Natter calls inside handlers and workers, and `@sync`/`Threads.@spawn` for fan-out or background work. Do not show exhaustive `_async` wrapper rewrites as the normal usage pattern.
- Automatic recovery from transient disconnects is a core client requirement. Reconnect should happen in the background, preserve user subscriptions, replay subscription commands, flush buffered publishes, and avoid requiring application code to recreate the client.
- Connection status uses `EnumX.jl`; compare against namespaced values such as `ConnectionStatus.CONNECTED`, not bare enum constants.
- Extend Base functions deliberately (`close`, `flush`, `fetch`) by importing them in `src/Natter.jl`; avoid accidental shadowing.

## Performance And Resource Discipline

- Treat long-running production use as the default. Avoid task, channel, socket, timer, and buffer leaks.
- Keep hot paths allocation-conscious: protocol parsing, publish serialization, dispatch, and fetch loops should avoid unnecessary temporary objects.
- Prefer type-stable data structures and method signatures. Use `Any` only at integration boundaries where the wire format or Julia runtime makes it unavoidable.
- Use fast standard data structures before adding abstractions. Avoid global mutable state.
- When changing parser, dispatch, reconnect, or JetStream fetch behavior, consider allocation impact and backpressure behavior, not only functional correctness.
- Close transports and channels deterministically in error, drain, and close paths.
- Parser, buffering, timeout, and production safety limits must be configurable through `ConnectOptions`, documented, and tested. Avoid hardcoded production knobs that users may need to tune.
- Timed-out `flush` waiters must not allow late PONGs to satisfy later flush calls. Any stale waiter strategy must remain bounded for long-running clients.

## Protocol And Semantics

- Validate protocol inputs locally before writing to the socket. Invalid subjects, queues, headers, sizes, and mutually exclusive options should fail predictably without relying on server disconnects.
- Size checks must match the bytes sent on the wire. For header publishes, enforce limits against header bytes plus payload bytes.
- Public verbs must match their names exactly. `create`, `update`, `delete`, `purge`, `drain`, and `close` should not silently perform a different operation on missing or existing resources.
- Do not claim a feature is supported because its config fields serialize. Protocol control messages, status messages, lifecycle semantics, cleanup behavior, and error mapping must be implemented and tested.
- Server status/control messages must be handled explicitly in consumer and fetch paths; they should not leak to user callbacks as ordinary data unless that is the documented API.

## Transport Concurrency

- Coordinate transport writes, swaps, and close through `client.write_lock`.
- If both `client.write_lock` and `client.lock` are needed, acquire `client.write_lock` first, then `client.lock`; keep that lock order consistent.
- Do not perform blocking socket, TLS, file descriptor, or protocol IO while holding `client.lock`. Snapshot shared state under `client.lock`, then do IO outside it or under `client.write_lock` as appropriate.
- Reconnect, close, and foreground publish/flush/ack paths must not race by writing to a transport while another task clears, replaces, or closes it.

## Tests

Unit and integration tests are defined with `TestItems.jl` in logical `test/*.jl` files and executed by `TestItemRunner.jl` from `test/runtests.jl`. Use focused names such as `test/core.jl`, `test/protocol.jl`, and `test/integration.jl`; do not add `_testitems` suffixes. Shared test setup belongs in `test/helpers.jl`. Do not add new top-level `@testset` suites in `runtests.jl`.

Every fix for protocol validation, resource limits, lifecycle semantics, or error mapping needs a focused unit test and, when a real server can observe the behavior, an integration test. Prefer tests that prove invalid inputs fail locally and do not destabilize the connection.

Run server-free tests before finishing:

```bash
julia --project=. -e 'using Pkg; Pkg.test()'
```

Run real-server integration tests when touching connection, reconnect, drain, JetStream, or KeyValue behavior:

```bash
docker run -d --rm --name natter-test-nats -p 42222:4222 nats:2.11-alpine -js
env NATTER_RUN_INTEGRATION=true NATTER_RUN_JETSTREAM=true NATTER_URL=nats://127.0.0.1:42222 julia --project=. -e 'using Pkg; Pkg.test()'
docker stop natter-test-nats
```

If a test container already exists, inspect it first instead of blindly replacing it.

## Documentation

Documentation is built with DocumenterVitepress from the dedicated `docs/` Julia project. Keep pages organized by user-facing concepts, not internal source files. Prefer concise guides plus complete examples over API dumps.

When changing public APIs, update the relevant guide, example, and `docs/src/reference.md`. When support status changes, update `docs/src/feature-coverage.md`.

Do not push, deploy docs, or rebuild generated docs unless the user explicitly asks for that. Source-only documentation edits do not require running the docs build.

The `Documentation` GitHub Actions workflow builds the Vitepress site and deploys pushes on `main` to the `gh-pages` branch. GitHub Pages serves the `gh-pages` branch from `/`; main-branch docs are deployed at the site root. The root Pages deployment depends on `Documenter.deploydocs(; target=joinpath("build", "1"), versions=nothing, ...)` plus generated `versions.js` and `siteinfo.js`; do not reintroduce a root redirect to `/dev/`.

When explicitly asked to validate docs changes, build the markdown documentation with:

```bash
julia --project=docs -e 'using Pkg; Pkg.develop(PackageSpec(path=pwd())); Pkg.instantiate()'
julia --project=docs docs/make.jl
```

Set `DOCUMENTER_BUILD_VITEPRESS=true` only when explicitly asked to build the Vitepress site. Generated docs and Node artifacts should stay out of source control.

## Feature Tracking

Update `docs/src/feature-coverage.md` whenever support changes. Be explicit about:

- Supported production paths.
- Partial support and remaining hardening work.
- Esoteric or intentionally ignored NATS features.
- Downgrade status when only configuration or a happy path exists; upgrade only after behavior, failure modes, and lifecycle cleanup are covered.

## Engineering Rules

- Keep edits focused and small enough to review.
- Add tests with behavior changes, especially parser, reconnect, drain, JetStream, and KV changes.
- Avoid broad refactors while fixing a narrow issue.
- Prefer structured parsing/serialization over ad hoc string manipulation when feasible.
- Keep generated artifacts out of source control. `Manifest.toml` is ignored for this package.
- Never swallow cleanup failures with empty handlers such as `try ... catch end`. Report cleanup failures, throw them, or wrap them in `CleanupError` and `CompositeException` so callers and `error_cb` can observe them.
