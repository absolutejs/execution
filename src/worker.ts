import type {
  EffectHandler,
  EffectProviderReconciliationReference,
  EffectStore,
} from "./types";
import { effectProviderReconciliationReferenceFromResult } from "./adapterExecution";

export class UnknownEffectOutcomeError extends Error {
  readonly reconciliationReference?: EffectProviderReconciliationReference;

  constructor(
    message = "Provider outcome is unknown",
    options?: {
      reconciliationReference?: EffectProviderReconciliationReference;
    },
  ) {
    super(message);
    this.name = "UnknownEffectOutcomeError";
    this.reconciliationReference = options?.reconciliationReference;
  }
}

/** Standalone compatibility worker. Production deployments should schedule
 * effects with `createExecutionOutboxDispatcher` and `@absolutejs/queue` so
 * queue-postgres owns worker leases, retries, delayed work, and dead letters. */
export const createEffectWorker = ({
  handlers,
  leaseMs = 30_000,
  maxAttempts = 5,
  now = Date.now,
  store,
  workerId,
}: {
  handlers: Record<string, EffectHandler>;
  leaseMs?: number;
  maxAttempts?: number;
  now?: () => number;
  store: EffectStore;
  workerId: string;
}) => ({
  runOnce: async () => {
    const effect = await store.claim(workerId, leaseMs, now());
    if (!effect) return undefined;
    const handler = handlers[effect.handler];
    if (!handler) {
      await store.fail(
        effect.effectId,
        workerId,
        { error: "Unknown effect handler", status: "dead_letter" },
        now(),
      );
      return effect.effectId;
    }
    const controller = new AbortController();
    let result: unknown;
    try {
      result = await handler.execute(effect.input, {
        actionId: effect.actionId,
        effectId: effect.effectId,
        idempotencyKey: effect.idempotencyKey,
        inputDigest: effect.inputDigest,
        ...(effect.runId ? { runId: effect.runId } : {}),
        signal: controller.signal,
        tenantId: effect.tenantId,
      });
      if (!(await store.succeed(effect.effectId, workerId, result, now()))) {
        throw new UnknownEffectOutcomeError(
          "Effect lease lost before completion",
        );
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Effect failed";
      const unknown = error instanceof UnknownEffectOutcomeError;
      const reconciliationReference = unknown
        ? (error.reconciliationReference ??
          effectProviderReconciliationReferenceFromResult(result))
        : undefined;
      if (unknown)
        await store.quarantineUnknown(
          effect.effectId,
          effect.attempts,
          {
            error: message,
            ...(reconciliationReference ? { reconciliationReference } : {}),
          },
          now(),
        );
      else
        await store.fail(
          effect.effectId,
          workerId,
          effect.attempts >= maxAttempts
            ? { error: message, status: "dead_letter" }
            : {
                availableAt:
                  now() + Math.min(60_000, 1_000 * 2 ** (effect.attempts - 1)),
                error: message,
                status: "pending",
              },
          now(),
        );
    }
    return effect.effectId;
  },
});
