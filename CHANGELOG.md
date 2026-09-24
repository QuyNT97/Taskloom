# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-09-24

### Added

- Immutable task state and persistent transaction builders.
- Plugin state, transaction filtering, reducers and appended transactions.
- Async runtime with task handles, cancellation and race-safe execution tokens.
- FIFO, concurrency, timeout, retry, priority and latest-by-key policies.
- `createTaskEngine` facade with atomic batch enqueueing and subscriptions.
- Strict TypeScript inference for workers and plugin factories.
- Architecture, runtime, plugin-authoring and engine documentation.

[0.1.0]: https://github.com/QuyNT97/Taskloom/releases/tag/v0.1.0
