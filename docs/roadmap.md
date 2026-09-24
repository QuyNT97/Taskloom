# Phased delivery

Each phase is implemented and verified separately; the initial spec is the full
project direction, not a request to generate all phases at once.

- Phase 1 — complete: domain, immutable state/transaction, typed keys, isolated plugin state.
- Phase 2 — complete: filter/apply/append pipeline and external-plugin architecture tests.
- Phase 3 — complete: runtime, scheduling extension, execution tokens, cancellation, handles.
- Phase 4 — complete: FIFO, concurrency, timeout, retry, priority, latest plugins.
- Phase 5: engine facade, inference, subscriptions, examples and complete docs.

Phase 1–2 contracts are documented and tested: immutable ownership, state
visibility, stale transaction rejection, append termination and atomicity.
Runtime consumes the full pipeline result before executing effects or settling
handles. Phase 4 composes the six initial policies through those public extensions.
The next step is Phase 5: the beginner facade, automatic type inference across
policy factories, subscriptions and complete examples.
