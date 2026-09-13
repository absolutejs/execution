import { Type } from "@sinclair/typebox";
import { effectProviderReconciliationReferenceFromResult } from "./adapterExecution";
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
    leaseMs = 30_000,
    heartbeatMs = Math.floor(leaseMs / 3),
    now = Date.now,
    store,
    workerId = crypto.randomUUID(),
  }: {
    handlers: Record<string, EffectHandler>;
    leaseMs?: number;
    heartbeatMs?: number;
    now?: () => number;
    store: EffectStore;
    workerId?: string;
  }): ExecutionQueueHandler =>
  async ({ effectId }, context) => {
    if (
      !Number.isSafeInteger(leaseMs) ||
      !Number.isSafeInteger(heartbeatMs) ||
      heartbeatMs < 1 ||
      heartbeatMs >= leaseMs
    )
      throw new Error("Invalid execution lease renewal interval");
    context.signal.throwIfAborted();
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
    const leaseOwner = `${workerId}:${crypto.randomUUID()}`;
    const effect = await store.claimEffect(
      effectId,
      leaseOwner,
      leaseMs,
      now(),
    );
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
      workerId: leaseOwner,
    });
    const handler = handlers[effect.handler];
    if (!handler) {
      const message = `Unknown effect handler: ${effect.handler}`;
      await store.finishAttempt(attemptId, "failed", now(), message);
      await store.fail(
        effectId,
        leaseOwner,
        { error: message, status: "dead_letter" },
        now(),
      );
      return;
    }
    const leaseAbort = new AbortController();
    const signal = AbortSignal.any([context.signal, leaseAbort.signal]);
    let renewal: Promise<void> | undefined;
    const renew = async () => {
      try {
        if (!(await store.heartbeat(effectId, leaseOwner, leaseMs, now())))
          throw new Error("Execution lease lost");
      } catch {
        leaseAbort.abort(
          new UnknownEffectOutcomeError(
            "Execution lease renewal failed; outcome requires reconciliation",
          ),
        );
      }
    };
    const timer = setInterval(() => {
      if (!renewal && !leaseAbort.signal.aborted)
        renewal = renew().finally(() => {
          renewal = undefined;
        });
    }, heartbeatMs);
    const stopRenewal = async () => {
      clearInterval(timer);
      await renewal;
    };
    let result: unknown;
    try {
      result = await handler.execute(effect.input, {
        actionId: effect.actionId,
        effectId: effect.effectId,
        idempotencyKey: effect.idempotencyKey,
        inputDigest: effect.inputDigest,
        ...(effect.runId ? { runId: effect.runId } : {}),
        signal,
        tenantId: effect.tenantId,
      });
      await stopRenewal();
      if (signal.aborted)
        throw new UnknownEffectOutcomeError(
          "Execution interrupted before completion was committed",
        );
      await store.finishAttempt(attemptId, "succeeded", now());
      if (!(await store.succeed(effectId, leaseOwner, result, now()))) {
        throw new UnknownEffectOutcomeError(
          "Provider succeeded but the local completion lease was lost",
        );
      }
    } catch (error) {
      await stopRenewal();
      const outcomeError = signal.aborted
        ? new UnknownEffectOutcomeError(
            "Execution interrupted; outcome requires reconciliation",
          )
        : error;
      const message =
        outcomeError instanceof Error
          ? outcomeError.message
          : String(outcomeError);
      if (outcomeError instanceof UnknownEffectOutcomeError) {
        const reconciliationReference =
          outcomeError.reconciliationReference ??
          effectProviderReconciliationReferenceFromResult(result);
        await store.finishAttempt(attemptId, "unknown", now(), message);
        await store.quarantineUnknown(
          effectId,
          effect.attempts,
          {
            error: message,
            ...(reconciliationReference ? { reconciliationReference } : {}),
          },
          now(),
        );
        return;
      }
      const dead = context.attempts + 1 >= context.maxAttempts;
      await store.finishAttempt(attemptId, "failed", now(), message);
      await store.fail(
        effectId,
        leaseOwner,
        {
          error: message,
          status: dead ? "dead_letter" : "failed",
        },
        now(),
      );
      if (!dead) throw error;
    } finally {
      await stopRenewal();
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
