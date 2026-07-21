# @absolutejs/execution

Crash-safe external effects for AI agents. Execution separates an agent's
durable intent from the fallible provider call that fulfills it.

The production path is PostgreSQL-first and reuses AbsoluteJS Queue:

1. `store.enqueue(effect)` atomically inserts the effect and an outbox event.
2. `createExecutionOutboxDispatcher()` idempotently moves that event into an
   `@absolutejs/queue` store (use `@absolutejs/queue-postgres` in production).
3. Queue owns scheduling, worker leases, retries, backoff, and dead letters.
4. `createExecutionQueueHandler()` owns provider idempotency, attempt history,
   results, unknown-outcome quarantine, and reconciliation.

Redis is not required. A process crash before outbox publish leaves the event
claimable; a crash after queue enqueue is harmless because the event id is also
the queue idempotency key.

```ts
import {
  createExecutionOutboxDispatcher,
  createExecutionQueueHandler,
  createPostgresEffectStore,
  executionJobs,
  executionPostgresSchemaSql,
  executionTenantInventoryPostgresSchemaSql,
} from "@absolutejs/execution";
import { createJobRegistry, createQueueWorker } from "@absolutejs/queue";

await sql.unsafe(executionPostgresSchemaSql());
await sql.unsafe(executionTenantInventoryPostgresSchemaSql());
const effects = createPostgresEffectStore({ client });
const dispatch = createExecutionOutboxDispatcher({
  store: effects,
  queue: queueStore,
});
const registry = createJobRegistry(executionJobs).on(
  "absolutejs.execution.effect",
  createExecutionQueueHandler({
    store: effects,
    handlers: {
      "email.send": {
        execute: (input, { idempotencyKey, signal }) =>
          email.send(input, { idempotencyKey, signal }),
      },
    },
  }),
);
const worker = createQueueWorker({ registry, store: queueStore });
```

If a provider times out after accepting a request, throw
`UnknownEffectOutcomeError`. Execution will not retry blindly: it quarantines
the effect as `unknown` until an authorized operator, provider query, or
provider webhook resolves it through `createEffectRecoveryOperations()`.
Recovery verifies evidence before one atomic status transition, retains an
append-only actor/source/evidence record, tenant-fences every case, and permits
retry only when evidence confirms the provider did not apply the effect. That
transition atomically creates a reconciliation-scoped outbox event so Queue can
actually schedule the safe retry after the original event was consumed. Apply
`effectRecoveryPostgresSchemaSql()` before using the PostgreSQL recovery API.
Successful effects can be reversed explicitly with
`compensateEffect()` when the handler provides `compensate`.

Verified provider webhooks and query results can enter recovery through
`createEffectEvidenceIngestion()`. The ingestion boundary retains only
normalized, tenant-bound evidence, authorizes that binding before persistence,
deduplicates provider delivery IDs, rejects
attempts to rebind an existing delivery to another effect, and resumes
reconciliation when a provider retries after a host crash. Production hosts
should apply `effectEvidencePostgresSchemaSql()` and use
`createPostgresEffectEvidenceStore()`. Raw webhook payloads and provider
credentials do not belong in the evidence store.

Agent Runtime hosts should use `createAgentRuntimeEffectExecutor()` rather than
returning an enqueue result as if it were a completed effect. The bridge stores
one tenant/run-fenced effect and asks Runtime to wait until the queue handler
records a terminal result. Its `authorize` callback must return the exact
Agency action ID and input digest retained for execution-time lease issuance.

## Certified adapters and tenant installations

`createEffectAdapterRegistry()` is the global fail-closed boundary. Adapter
descriptors declare effects, destinations, credential slots, idempotency,
reconciliation, compensation, and spend authority. Activation requires a fresh
certificate for the exact descriptor digest and host-verified evidence.

`createEffectAdapterInstallationRegistry()` narrows that certified authority for
one tenant. Every installation pins the adapter version and digest, starts
disabled, selects an effect and destination subset, maps required adapter
credential slots to host-owned secret aliases without storing values, and can
bind a per-effect spend ceiling to a mandate. Enabling and every authorization
recheck the global adapter, descriptor pin, credential aliases, mandate,
destination, effect, tenant, and spend ceiling. Descriptor drift or revoked
evidence therefore stops every dependent installation without a rollout.

Production hosts should apply
`effectAdapterRegistryPostgresSchemaSql()` and
`effectAdapterInstallationsPostgresSchemaSql()`, then use the corresponding
PostgreSQL stores. The host callbacks remain responsible for resolving secret
aliases and validating mandates; raw credentials never enter either registry.

`createEffectAdapterExecutionHandler()` is the final provider boundary. It
accepts an installation envelope, reauthorizes the exact tenant, effect,
destination, and spend, first recomputes the canonical authorized input digest,
verifies that the runtime driver's identity, version,
idempotency, reconciliation, and compensation capabilities match the certified
descriptor, and only then resolves the authorized secret aliases. Credential
values exist only in the driver context and are never placed in the durable
installation record or bridge result. Unknown provider outcomes should be
raised as `UnknownEffectOutcomeError` so the queue quarantines them for
reconciliation instead of retrying an ambiguous side effect.
