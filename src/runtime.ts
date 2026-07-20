import { AgentEffectDeferredError } from "@absolutejs/agent-runtime";
import type { AgentEffectExecutor } from "@absolutejs/agent-runtime";
import type { EffectRecord, EffectStore } from "./types";

export class TerminalEffectError extends Error {
  readonly status: EffectRecord["status"];

  constructor(effect: EffectRecord) {
    super(effect.error ?? `Effect ended in ${effect.status}`);
    this.name = "TerminalEffectError";
    this.status = effect.status;
  }
}

const terminalFailure = new Set<EffectRecord["status"]>([
  "compensation_failed",
  "dead_letter",
  "unknown",
]);

export const createAgentRuntimeEffectExecutor = (options: {
  authorize: (
    input: Parameters<AgentEffectExecutor["execute"]>[0],
  ) => Promise<{ actionId: string; inputDigest: string }>;
  now?: () => number;
  pollAfterMs?: number;
  store: EffectStore;
}): AgentEffectExecutor => {
  const now = options.now ?? Date.now;
  const pollAfterMs = options.pollAfterMs ?? 1_000;

  return {
    execute: async (input) => {
      const effectId = `runtime:${input.run.id}:${input.step.id}`;
      const existing = await options.store.get(effectId);
      if (
        existing?.status === "succeeded" ||
        existing?.status === "compensated"
      )
        return existing.result;
      if (existing && terminalFailure.has(existing.status))
        throw new TerminalEffectError(existing);
      if (!existing) {
        const authorization = await options.authorize(input);
        const timestamp = now();
        const effect: EffectRecord = {
          actionId: authorization.actionId,
          attempts: 0,
          availableAt: timestamp,
          createdAt: timestamp,
          effectId,
          handler: input.name,
          idempotencyKey: input.idempotencyKey,
          input: input.payload,
          inputDigest: authorization.inputDigest,
          runId: input.run.id,
          status: "pending",
          tenantId: input.run.actor.tenantId,
          updatedAt: timestamp,
        };
        if (!(await options.store.enqueue(effect))) {
          const duplicate = await options.store.getByIdempotencyKey(
            effect.tenantId,
            effect.idempotencyKey,
          );
          if (!duplicate || duplicate.effectId !== effect.effectId)
            throw new Error(
              "Effect idempotency key belongs to another request",
            );
        }
      }
      throw new AgentEffectDeferredError(
        new Date(now() + pollAfterMs).toISOString(),
      );
    },
  };
};
