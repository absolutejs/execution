# @absolutejs/execution

Crash-safe external effect execution for agents. Effects are idempotency-bound,
claimed with expiring worker leases, retried with backoff, dead-lettered after a
bounded attempt count, or quarantined as `unknown` when a provider may have
accepted the request before connectivity was lost. Unknown outcomes require
explicit reconciliation and are never blindly retried.

Use `executionPostgresSchemaSql()` for durable state and implement `EffectStore`
with conditional claim/update statements. The bundled memory store is for tests.
