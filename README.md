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
the effect as `unknown` until an operator or provider webhook calls
`store.reconcile(...)`. Successful effects can be reversed explicitly with
`compensateEffect()` when the handler provides `compensate`.

Agent Runtime hosts should use `createAgentRuntimeEffectExecutor()` rather than
returning an enqueue result as if it were a completed effect. The bridge stores
one tenant/run-fenced effect and asks Runtime to wait until the queue handler
records a terminal result. Its `authorize` callback must return the exact
Agency action ID and input digest retained for execution-time lease issuance.
