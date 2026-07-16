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
} from "@absolutejs/execution";
import { createJobRegistry, createQueueWorker } from "@absolutejs/queue";

await sql.unsafe(executionPostgresSchemaSql());
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
