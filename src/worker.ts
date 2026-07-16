import type { EffectHandler, EffectStore } from "./types";

export class UnknownEffectOutcomeError extends Error {
  constructor(message = "Provider outcome is unknown") {
    super(message);
    this.name = "UnknownEffectOutcomeError";
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
    try {
      const result = await handler.execute(effect.input, {
        idempotencyKey: effect.idempotencyKey,
        signal: controller.signal,
      });
      if (!(await store.succeed(effect.effectId, workerId, result, now()))) {
        throw new UnknownEffectOutcomeError(
          "Effect lease lost before completion",
        );
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Effect failed";
      const unknown = error instanceof UnknownEffectOutcomeError;
      await store.fail(
        effect.effectId,
        workerId,
        unknown
          ? { error: message, status: "unknown" }
          : effect.attempts >= maxAttempts
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
