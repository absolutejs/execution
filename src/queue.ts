import { Type } from "@sinclair/typebox";
import { UnknownEffectOutcomeError } from "./worker";
import type {
  EffectHandler,
  EffectStore,
  ExecutionQueueHandler,
  ExecutionQueueStore,
} from "./types";

export const executionJobs = {
  "absolutejs.execution.effect": Type.Object({ effectId: Type.String() }),
} as const;

export const createExecutionOutboxDispatcher = ({
  leaseMs = 30_000,
  maxAttempts = 5,
  now = Date.now,
  queue,
  store,
  workerId = crypto.randomUUID(),
}: {
  leaseMs?: number;
  maxAttempts?: number;
  now?: () => number;
  queue: ExecutionQueueStore;
  store: EffectStore;
  workerId?: string;
}) => ({
  runOnce: async () => {
    const event = await store.claimOutbox(workerId, leaseMs, now());
    if (!event) return undefined;
    try {
      await queue.enqueue({
        idempotencyKey: event.eventId,
        kind: "absolutejs.execution.effect",
        maxAttempts,
        payload: { effectId: event.effectId },
      });
      if (!(await store.publishOutbox(event.eventId, workerId))) {
        throw new Error("Execution outbox lease lost before publish commit");
      }
    } catch (error) {
      await store.retryOutbox(event.eventId, workerId);
      throw error;
    }
    return event.eventId;
  },
});

export const createExecutionQueueHandler =
  ({
    handlers,
    now = Date.now,
    store,
    workerId = crypto.randomUUID(),
  }: {
    handlers: Record<string, EffectHandler>;
    now?: () => number;
    store: EffectStore;
    workerId?: string;
  }): ExecutionQueueHandler =>
  async ({ effectId }, context) => {
    const current = await store.get(effectId);
    if (
      !current ||
      current.status === "succeeded" ||
      current.status === "compensated"
    ) {
      return;
    }
    if (current.status === "unknown" || current.status === "dead_letter") {
      return;
    }
    const effect = await store.claimEffect(effectId, workerId, 30_000, now());
    if (!effect) {
      throw new Error(`Effect ${effectId} is not claimable`);
    }
    const attemptId = crypto.randomUUID();
    await store.recordAttempt({
      attemptId,
      effectId,
      kind: "execute",
      number: effect.attempts,
      outcome: "running",
      startedAt: now(),
      workerId,
    });
    const handler = handlers[effect.handler];
    if (!handler) {
      const message = `Unknown effect handler: ${effect.handler}`;
      await store.finishAttempt(attemptId, "failed", now(), message);
      await store.fail(
        effectId,
        workerId,
        { error: message, status: "dead_letter" },
        now(),
      );
      return;
    }
    try {
      const result = await handler.execute(effect.input, {
        actionId: effect.actionId,
        effectId: effect.effectId,
        idempotencyKey: effect.idempotencyKey,
        inputDigest: effect.inputDigest,
        ...(effect.runId ? { runId: effect.runId } : {}),
        signal: context.signal,
        tenantId: effect.tenantId,
      });
      await store.finishAttempt(attemptId, "succeeded", now());
      if (!(await store.succeed(effectId, workerId, result, now()))) {
        throw new UnknownEffectOutcomeError(
          "Provider succeeded but the local completion lease was lost",
        );
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (error instanceof UnknownEffectOutcomeError) {
        await store.finishAttempt(attemptId, "unknown", now(), message);
        await store.fail(
          effectId,
          workerId,
          { error: message, status: "unknown" },
          now(),
        );
        return;
      }
      const dead = context.attempts + 1 >= context.maxAttempts;
      await store.finishAttempt(attemptId, "failed", now(), message);
      await store.fail(
        effectId,
        workerId,
        {
          error: message,
          status: dead ? "dead_letter" : "failed",
        },
        now(),
      );
      if (!dead) throw error;
    }
  };

export const compensateEffect = async ({
  effectId,
  handlers,
  now = Date.now,
  signal = new AbortController().signal,
  store,
  workerId = crypto.randomUUID(),
}: {
  effectId: string;
  handlers: Record<string, EffectHandler>;
  now?: () => number;
  signal?: AbortSignal;
  store: EffectStore;
  workerId?: string;
}) => {
  const effect = await store.startCompensation(effectId, workerId, now());
  if (!effect) return false;
  const compensate = handlers[effect.handler]?.compensate;
  if (!compensate) {
    await store.finishCompensation(
      effectId,
      workerId,
      now(),
      `No compensation handler: ${effect.handler}`,
    );
    return false;
  }
  const attemptId = crypto.randomUUID();
  await store.recordAttempt({
    attemptId,
    effectId,
    kind: "compensate",
    number: effect.attempts + 1,
    outcome: "running",
    startedAt: now(),
    workerId,
  });
  try {
    await compensate(effect.result, {
      actionId: effect.actionId,
      effectId: effect.effectId,
      idempotencyKey: `${effect.idempotencyKey}:compensate`,
      inputDigest: effect.inputDigest,
      ...(effect.runId ? { runId: effect.runId } : {}),
      signal,
      tenantId: effect.tenantId,
    });
    await store.finishAttempt(attemptId, "succeeded", now());
    return await store.finishCompensation(effectId, workerId, now());
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await store.finishAttempt(attemptId, "failed", now(), message);
    await store.finishCompensation(effectId, workerId, now(), message);
    throw error;
  }
};
