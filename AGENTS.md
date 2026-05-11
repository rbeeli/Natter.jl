# Agent Guidance For Natter.jl

Natter.jl is a pure Julia NATS client implementation.

## Source Layout

- `src/Natter.jl`: module entrypoint, imports, exports, constants, and includes only.
- `src/errors.jl`: exception types and `showerror` methods.
- `src/types.jl`: shared public and internal data types.
- `src/protocol.jl`: NATS wire parsing, command serialization, header handling, subject validation, and byte conversion.
- `src/connection.jl`: connect handshake, TLS wrapping, reader/ping/reconnect loops, discovered servers, and pending replay.
- `src/core.jl`: publish, subscribe, request/reply, flush, drain, close, slow-consumer dispatch, and inbox generation.
- `src/jetstream.jl`: JetStream management, publish ack, consumers, pull/push subscriptions, message metadata, and ack verbs.
- `src/keyvalue.jl`: KeyValue bucket and key operations built on JetStream.
- `docs/`: DocumenterVitepress documentation subproject. Source pages live in `docs/src`; the docs entrypoint is `docs/make.jl`.

Do not put substantial implementation back into `src/Natter.jl`. New features should go into the closest existing file or a new focused file included from `src/Natter.jl`.

## Compatibility And API

- Julia target is `1.10+`.
- Backward compatibility is not a constraint yet; prefer correctness and maintainability.
- APIs should be ergonomic and Julia-idiomatic: prefer clear method signatures, keyword arguments for optional behavior, concrete return types where practical, and Base extensions only when they fit established Julia conventions.
- Public APIs are synchronous/convenience-first. Internals use Julia tasks for reader, ping, reconnect, and callback processing.
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

## Tests

Unit and integration tests are defined with `TestItems.jl` in logical `test/*.jl` files and executed by `TestItemRunner.jl` from `test/runtests.jl`. Use focused names such as `test/core.jl`, `test/protocol.jl`, and `test/integration.jl`; do not add `_testitems` suffixes. Shared test setup belongs in `test/helpers.jl`. Do not add new top-level `@testset` suites in `runtests.jl`.

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

When changing public APIs, update the relevant guide, example, and `docs/src/reference.md`. When support status changes, update both `FEATURES_COVERAGE.md` and `docs/src/feature-coverage.md`.

The `Documentation` GitHub Actions workflow builds the Vitepress site and deploys pushes on `main` to the `gh-pages` branch. GitHub Pages serves the `gh-pages` branch from `/`; main-branch docs are deployed at the site root.

Build the markdown documentation before finishing docs changes:

```bash
julia --project=docs -e 'using Pkg; Pkg.develop(PackageSpec(path=pwd())); Pkg.instantiate()'
julia --project=docs docs/make.jl
```

Set `DOCUMENTER_BUILD_VITEPRESS=true` to also build the Vitepress site. Generated docs and Node artifacts should stay out of source control.

## Feature Tracking

Update `FEATURES_COVERAGE.md` whenever support changes. Be explicit about:

- Supported production paths.
- Partial support and remaining hardening work.
- Esoteric or intentionally ignored NATS features.

## Engineering Rules

- Keep edits focused and small enough to review.
- Add tests with behavior changes, especially parser, reconnect, drain, JetStream, and KV changes.
- Avoid broad refactors while fixing a narrow issue.
- Prefer structured parsing/serialization over ad hoc string manipulation when feasible.
- Keep generated artifacts out of source control. `Manifest.toml` is ignored for this package.
