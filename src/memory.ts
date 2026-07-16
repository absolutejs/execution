import type {
  EffectAttempt,
  EffectOutboxRecord,
  EffectRecord,
  EffectStore,
} from "./types";

export const createMemoryEffectStore = (): EffectStore => {
  const rows = new Map<string, EffectRecord>();
  const outbox = new Map<string, EffectOutboxRecord>();
  const attempts = new Map<string, EffectAttempt>();
  let tail = Promise.resolve();
  const locked = async <T>(run: () => T | Promise<T>) => {
    const previous = tail;
    let release = () => {};
    tail = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await run();
    } finally {
      release();
    }
  };

  return {
    enqueue: (effect) =>
      locked(() => {
        if (
          [...rows.values()].some(
            (row) => row.idempotencyKey === effect.idempotencyKey,
          )
        ) {
          return false;
        }
        rows.set(effect.effectId, structuredClone(effect));
        outbox.set(`effect:${effect.effectId}`, {
          attempts: 0,
          effectId: effect.effectId,
          eventId: `effect:${effect.effectId}`,
        });
        return true;
      }),
    claim: (workerId, leaseMs, now) =>
      locked(() => {
        const row = [...rows.values()].find(
          (candidate) =>
            (candidate.status === "pending" ||
              candidate.status === "failed" ||
              (candidate.status === "leased" &&
                (candidate.leaseExpiresAt ?? 0) <= now)) &&
            candidate.availableAt <= now,
        );
        if (!row) return undefined;
        const next: EffectRecord = {
          ...row,
          attempts: row.attempts + 1,
          leaseExpiresAt: now + leaseMs,
          leaseOwner: workerId,
          status: "leased",
          updatedAt: now,
        };
        rows.set(row.effectId, next);
        return structuredClone(next);
      }),
    claimEffect: (effectId, workerId, leaseMs, now) =>
      locked(() => {
        const row = rows.get(effectId);
        if (
          !row ||
          row.availableAt > now ||
          !(
            row.status === "pending" ||
            row.status === "failed" ||
            (row.status === "leased" && (row.leaseExpiresAt ?? 0) <= now)
          )
        ) {
          return undefined;
        }
        const next: EffectRecord = {
          ...row,
          attempts: row.attempts + 1,
          leaseExpiresAt: now + leaseMs,
          leaseOwner: workerId,
          status: "leased",
          updatedAt: now,
        };
        rows.set(effectId, next);
        return structuredClone(next);
      }),
    claimOutbox: (workerId, leaseMs, now) =>
      locked(() => {
        const event = [...outbox.values()].find(
          (candidate) =>
            !candidate.leaseOwner || (candidate.leaseExpiresAt ?? 0) <= now,
        );
        if (!event) return undefined;
        const next = {
          ...event,
          attempts: event.attempts + 1,
          leaseExpiresAt: now + leaseMs,
          leaseOwner: workerId,
        };
        outbox.set(event.eventId, next);
        return structuredClone(next);
      }),
    fail: (effectId, workerId, update, now) =>
      locked(() => {
        const row = rows.get(effectId);
        if (!row || row.status !== "leased" || row.leaseOwner !== workerId) {
          return false;
        }
        rows.set(effectId, {
          ...row,
          ...update,
          availableAt: update.availableAt ?? row.availableAt,
          leaseExpiresAt: undefined,
          leaseOwner: undefined,
          updatedAt: now,
        });
        return true;
      }),
    finishAttempt: (attemptId, outcome, now, error) =>
      locked(() => {
        const attempt = attempts.get(attemptId);
        if (!attempt) return;
        attempts.set(attemptId, {
          ...attempt,
          ...(error === undefined ? {} : { error }),
          finishedAt: now,
          outcome,
        });
      }),
    finishCompensation: (effectId, workerId, now, error) =>
      locked(() => {
        const row = rows.get(effectId);
        if (
          !row ||
          row.status !== "compensating" ||
          row.leaseOwner !== workerId
        ) {
          return false;
        }
        rows.set(effectId, {
          ...row,
          ...(error === undefined ? {} : { error }),
          leaseOwner: undefined,
          status: error === undefined ? "compensated" : "compensation_failed",
          updatedAt: now,
        });
        return true;
      }),
    get: async (effectId) => {
      const row = rows.get(effectId);
      return row ? structuredClone(row) : undefined;
    },
    heartbeat: (effectId, workerId, leaseMs, now) =>
      locked(() => {
        const row = rows.get(effectId);
        if (!row || row.status !== "leased" || row.leaseOwner !== workerId) {
          return false;
        }
        rows.set(effectId, {
          ...row,
          leaseExpiresAt: now + leaseMs,
          updatedAt: now,
        });
        return true;
      }),
    listAttempts: async (effectId) =>
      [...attempts.values()]
        .filter((attempt) => attempt.effectId === effectId)
        .sort((left, right) => left.startedAt - right.startedAt)
        .map((attempt) => structuredClone(attempt)),
    publishOutbox: (eventId, workerId) =>
      locked(() => {
        const event = outbox.get(eventId);
        if (!event || event.leaseOwner !== workerId) return false;
        outbox.delete(eventId);
        return true;
      }),
    reconcile: (effectId, update, now) =>
      locked(() => {
        const row = rows.get(effectId);
        if (!row || row.status !== "unknown") return false;
        rows.set(effectId, { ...row, ...update, updatedAt: now });
        return true;
      }),
    recordAttempt: (attempt) =>
      locked(() => {
        attempts.set(attempt.attemptId, structuredClone(attempt));
      }),
    retryOutbox: (eventId, workerId) =>
      locked(() => {
        const event = outbox.get(eventId);
        if (!event || event.leaseOwner !== workerId) return false;
        outbox.set(eventId, {
          ...event,
          leaseExpiresAt: undefined,
          leaseOwner: undefined,
        });
        return true;
      }),
    startCompensation: (effectId, workerId, now) =>
      locked(() => {
        const row = rows.get(effectId);
        if (
          !row ||
          (row.status !== "succeeded" && row.status !== "compensation_failed")
        ) {
          return undefined;
        }
        const next: EffectRecord = {
          ...row,
          leaseOwner: workerId,
          status: "compensating",
          updatedAt: now,
        };
        rows.set(effectId, next);
        return structuredClone(next);
      }),
    succeed: (effectId, workerId, result, now) =>
      locked(() => {
        const row = rows.get(effectId);
        if (!row || row.status !== "leased" || row.leaseOwner !== workerId) {
          return false;
        }
        rows.set(effectId, {
          ...row,
          leaseExpiresAt: undefined,
          leaseOwner: undefined,
          result,
          status: "succeeded",
          updatedAt: now,
        });
        return true;
      }),
  };
};
