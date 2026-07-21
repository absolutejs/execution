import type {
  EffectAttempt,
  EffectRecord,
  EffectReconciliationRecord,
  EffectReconciliationResolution,
  EffectReconciliationSource,
  EffectRecoveryStore,
} from "./types";

export type EffectRecoveryRequest = {
  actorId: string;
  effectId: string;
  evidenceReference: string;
  note: string;
  resolution: EffectReconciliationResolution;
  source: EffectReconciliationSource;
  tenantId: string;
};

export type EffectRecoveryCase = {
  actionId: string;
  attempts: number;
  attemptHistory: ReadonlyArray<EffectAttempt>;
  createdAt: number;
  effectId: string;
  error?: string;
  handler: string;
  reconciliationHistory: ReadonlyArray<EffectReconciliationRecord>;
  runId?: string;
  status: EffectRecord["status"];
  tenantId: string;
  updatedAt: number;
};

export class EffectRecoveryError extends Error {}

const required = (value: string, field: string) => {
  const normalized = value.trim();
  if (!normalized) throw new EffectRecoveryError(`${field} is required`);
  return normalized;
};

const summary = async (
  store: EffectRecoveryStore,
  effect: EffectRecord,
): Promise<EffectRecoveryCase> => ({
  actionId: effect.actionId,
  attempts: effect.attempts,
  attemptHistory: await store.listAttempts(effect.effectId),
  createdAt: effect.createdAt,
  effectId: effect.effectId,
  ...(effect.error ? { error: effect.error } : {}),
  handler: effect.handler,
  reconciliationHistory: await store.listReconciliations(effect.effectId),
  ...(effect.runId ? { runId: effect.runId } : {}),
  status: effect.status,
  tenantId: effect.tenantId,
  updatedAt: effect.updatedAt,
});

const transition = (request: EffectRecoveryRequest) => {
  switch (request.resolution) {
    case "confirmed_succeeded":
      return {
        result: { evidenceReference: request.evidenceReference },
        status: "succeeded" as const,
      };
    case "confirmed_not_applied":
      return {
        error: "Provider evidence confirmed the effect was not applied",
        status: "pending" as const,
      };
    case "dead_letter":
      return {
        error: "Operator marked the unknown effect unrecoverable",
        status: "dead_letter" as const,
      };
  }
};

export const createEffectRecoveryOperations = (options: {
  authorize: (input: {
    actorId: string;
    effect: EffectRecord;
    resolution: EffectReconciliationResolution;
  }) => Promise<boolean>;
  id?: () => string;
  now?: () => number;
  store: EffectRecoveryStore;
  verifyEvidence: (input: {
    effect: EffectRecord;
    evidenceReference: string;
    resolution: EffectReconciliationResolution;
    source: EffectReconciliationSource;
  }) => Promise<boolean>;
}) => {
  const now = options.now ?? Date.now;
  const id = options.id ?? crypto.randomUUID;

  const getUnknown = async (effectId: string, tenantId: string) => {
    const effect = await options.store.get(required(effectId, "effectId"));
    if (!effect || effect.tenantId !== required(tenantId, "tenantId"))
      throw new EffectRecoveryError("Unknown effect was not found");
    if (effect.status !== "unknown")
      throw new EffectRecoveryError("Only unknown effects can be reconciled");
    return effect;
  };

  return {
    inventory: async (input: { limit: number; tenantId?: string }) => {
      if (!Number.isSafeInteger(input.limit) || input.limit < 1)
        throw new EffectRecoveryError("limit must be a positive integer");
      const effects = await options.store.list({
        limit: input.limit,
        status: "unknown",
        ...(input.tenantId
          ? { tenantId: required(input.tenantId, "tenantId") }
          : {}),
      });
      return Promise.all(
        effects.map((effect) => summary(options.store, effect)),
      );
    },
    resolve: async (input: EffectRecoveryRequest) => {
      const request = {
        ...input,
        actorId: required(input.actorId, "actorId"),
        effectId: required(input.effectId, "effectId"),
        evidenceReference: required(
          input.evidenceReference,
          "evidenceReference",
        ),
        note: required(input.note, "note"),
        tenantId: required(input.tenantId, "tenantId"),
      };
      const effect = await getUnknown(request.effectId, request.tenantId);
      if (
        !(await options.authorize({
          actorId: request.actorId,
          effect,
          resolution: request.resolution,
        }))
      )
        throw new EffectRecoveryError(
          "Effect reconciliation was not authorized",
        );
      if (
        !(await options.verifyEvidence({
          effect,
          evidenceReference: request.evidenceReference,
          resolution: request.resolution,
          source: request.source,
        }))
      )
        throw new EffectRecoveryError(
          "Effect reconciliation evidence is invalid",
        );

      const createdAt = now();
      const reconciliation: EffectReconciliationRecord = {
        actorId: request.actorId,
        createdAt,
        effectId: effect.effectId,
        evidenceReference: request.evidenceReference,
        note: request.note,
        reconciliationId: id(),
        resolution: request.resolution,
        source: request.source,
        tenantId: effect.tenantId,
      };
      if (
        !(await options.store.resolveUnknown({
          effectId: effect.effectId,
          reconciliation,
          ...transition(request),
          updatedAt: createdAt,
        }))
      )
        throw new EffectRecoveryError(
          "Unknown effect was already reconciled concurrently",
        );

      const resolved = await options.store.get(effect.effectId);
      if (!resolved)
        throw new EffectRecoveryError("Reconciled effect was not found");
      return summary(options.store, resolved);
    },
  };
};
