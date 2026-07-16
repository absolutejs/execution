import { describe, expect, test } from "bun:test";
import {
  createInMemoryJobStore,
  createJobRegistry,
  createQueueWorker,
} from "@absolutejs/queue";
import {
  compensateEffect,
  createEffectWorker,
  createExecutionOutboxDispatcher,
  createExecutionQueueHandler,
  createMemoryEffectStore,
  executionJobs,
  UnknownEffectOutcomeError,
  type EffectRecord,
  type ExecutionQueueStore,
} from "../src";

const effect = (id: string): EffectRecord => ({
  actionId: "action-1",
  attempts: 0,
  availableAt: 0,
  createdAt: 0,
  effectId: id,
  handler: "send",
  idempotencyKey: id,
  input: { message: "hello" },
  inputDigest: "sha256:digest",
  status: "pending",
  updatedAt: 0,
});

describe("standalone effect worker", () => {
  test("does not double claim and records success", async () => {
    const store = createMemoryEffectStore();
    await store.enqueue(effect("e1"));
    let calls = 0;
    const worker = createEffectWorker({
      handlers: { send: { execute: async () => ({ calls: ++calls }) } },
      now: () => 1,
      store,
      workerId: "worker-1",
    });
    await Promise.all([worker.runOnce(), worker.runOnce()]);
    expect(calls).toBe(1);
    expect((await store.get("e1"))?.status).toBe("succeeded");
  });

  test("quarantines unknown outcomes for reconciliation", async () => {
    const store = createMemoryEffectStore();
    await store.enqueue(effect("e2"));
    const worker = createEffectWorker({
      handlers: {
        send: {
          execute: async () => {
            throw new UnknownEffectOutcomeError();
          },
        },
      },
      now: () => 1,
      store,
      workerId: "worker-1",
    });
    await worker.runOnce();
    expect((await store.get("e2"))?.status).toBe("unknown");
    expect(
      await store.reconcile(
        "e2",
        { result: { providerId: "p" }, status: "succeeded" },
        2,
      ),
    ).toBe(true);
  });
});

describe("queue bridge and transactional outbox", () => {
  test("dispatches one idempotent queue job and executes it", async () => {
    const store = createMemoryEffectStore();
    await store.enqueue(effect("queued-1"));
    const jobs: Parameters<ExecutionQueueStore["enqueue"]>[0][] = [];
    const queue: ExecutionQueueStore = {
      enqueue: async (job) => {
        jobs.push(job);
        return crypto.randomUUID();
      },
    };
    const dispatcher = createExecutionOutboxDispatcher({
      now: () => 1,
      queue,
      store,
      workerId: "outbox-1",
    });
    expect(await dispatcher.runOnce()).toBe("effect:queued-1");
    expect(await dispatcher.runOnce()).toBeUndefined();
    expect(jobs[0]?.idempotencyKey).toBe("effect:queued-1");

    const handler = createExecutionQueueHandler({
      handlers: { send: { execute: async () => ({ providerId: "p1" }) } },
      now: () => 2,
      store,
      workerId: "effect-1",
    });
    await handler(
      { effectId: "queued-1" },
      {
        attempts: 0,
        id: crypto.randomUUID(),
        kind: "absolutejs.execution.effect",
        maxAttempts: 5,
        signal: new AbortController().signal,
      },
    );
    expect((await store.get("queued-1"))?.result).toEqual({ providerId: "p1" });
  });

  test("is structurally compatible with the real AbsoluteJS queue worker", async () => {
    const effects = createMemoryEffectStore();
    await effects.enqueue(effect("real-queue"));
    const queueStore = createInMemoryJobStore(executionJobs);
    const dispatcher = createExecutionOutboxDispatcher({
      queue: queueStore,
      store: effects,
    });
    await dispatcher.runOnce();
    const registry = createJobRegistry(executionJobs).on(
      "absolutejs.execution.effect",
      createExecutionQueueHandler({
        handlers: { send: { execute: async () => "delivered" } },
        store: effects,
      }),
    );
    await createQueueWorker({ registry, store: queueStore }).runOnce();
    expect((await effects.get("real-queue"))?.status).toBe("succeeded");
  });

  test("returns a failed outbox lease for retry when queue enqueue fails", async () => {
    const store = createMemoryEffectStore();
    await store.enqueue(effect("queued-2"));
    let calls = 0;
    const queue: ExecutionQueueStore = {
      enqueue: async () => {
        calls += 1;
        if (calls === 1) throw new Error("queue unavailable");
        return crypto.randomUUID();
      },
    };
    const dispatcher = createExecutionOutboxDispatcher({ queue, store });
    await expect(dispatcher.runOnce()).rejects.toThrow("queue unavailable");
    expect(await dispatcher.runOnce()).toBe("effect:queued-2");
  });
});

test("compensation is explicit, idempotency-scoped, and recorded", async () => {
  const store = createMemoryEffectStore();
  await store.enqueue(effect("comp-1"));
  const claimed = await store.claimEffect("comp-1", "execute", 1_000, 1);
  expect(claimed).toBeDefined();
  await store.succeed("comp-1", "execute", { providerId: "p1" }, 2);
  let key = "";
  expect(
    await compensateEffect({
      effectId: "comp-1",
      handlers: {
        send: {
          execute: async () => undefined,
          compensate: async (_result, context) => {
            key = context.idempotencyKey;
          },
        },
      },
      now: () => 3,
      store,
      workerId: "compensator",
    }),
  ).toBe(true);
  expect(key).toBe("comp-1:compensate");
  expect((await store.get("comp-1"))?.status).toBe("compensated");
  expect((await store.listAttempts("comp-1"))[0]?.kind).toBe("compensate");
});
