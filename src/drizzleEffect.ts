import { and, asc, desc, eq, inArray, isNull, lte, or } from "drizzle-orm";
import type { PgAsyncDatabase } from "drizzle-orm/pg-core";
import { encodedJsonb, executionDrizzleSchema } from "./drizzleSchema";
import type {
  EffectAttempt,
  EffectRecord,
  EffectReconciliationRecord,
  EffectRecoveryStore,
  EffectStore,
} from "./types";

type AnyPgDatabase = PgAsyncDatabase<any, any>;
type Tables = ReturnType<typeof executionDrizzleSchema>;
type EffectRow = Tables["effects"]["$inferSelect"];

const effectRecord = (row: EffectRow): EffectRecord => ({
  ...row.data,
  attempts: row.attempts,
});
const withoutLease = (record: EffectRecord) => {
  const { leaseExpiresAt, leaseOwner, ...value } = record;
  void leaseExpiresAt;
  void leaseOwner;
  return value;
};
const attemptRecord = (
  row: Tables["attempts"]["$inferSelect"],
): EffectAttempt => ({
  attemptId: row.attempt_id,
  effectId: row.effect_id,
  ...(row.error === null ? {} : { error: row.error }),
  ...(row.finished_at === null ? {} : { finishedAt: row.finished_at }),
  kind: row.kind as EffectAttempt["kind"],
  number: row.number,
  outcome: row.outcome as EffectAttempt["outcome"],
  startedAt: row.started_at,
  workerId: row.worker_id,
});
const reconciliationRecord = (
  row: Tables["reconciliations"]["$inferSelect"],
): EffectReconciliationRecord => ({
  actorId: row.actor_id,
  createdAt: row.created_at,
  effectId: row.effect_id,
  evidenceReference: row.evidence_reference,
  note: row.note,
  reconciliationId: row.reconciliation_id,
  resolution: row.resolution as EffectReconciliationRecord["resolution"],
  source: row.source as EffectReconciliationRecord["source"],
  tenantId: row.tenant_id,
});

export const createDrizzleEffectStore = <DB extends AnyPgDatabase>(
  db: DB,
  options: { namespace?: string } = {},
): EffectStore & EffectRecoveryStore => {
  const { attempts, effects, outbox, reconciliations } = executionDrizzleSchema(
    options.namespace,
  );
  const updateLeased = (
    effectId: string,
    workerId: string,
    update: Partial<EffectRecord> & { status: EffectRecord["status"] },
    now: number,
  ) =>
    db.transaction(async (transaction) => {
      const [row] = await transaction
        .select()
        .from(effects)
        .where(
          and(
            eq(effects.effect_id, effectId),
            eq(effects.lease_owner, workerId),
            eq(effects.status, "leased"),
          ),
        )
        .for("update")
        .limit(1);
      if (!row) return false;
      const next: EffectRecord = {
        ...withoutLease(effectRecord(row)),
        ...update,
        status: update.status,
        updatedAt: now,
      };
      return (
        (
          await transaction
            .update(effects)
            .set({
              available_at: next.availableAt,
              data: encodedJsonb(next),
              lease_expires_at: null,
              lease_owner: null,
              status: next.status,
              updated_at: now,
            })
            .where(
              and(
                eq(effects.effect_id, effectId),
                eq(effects.lease_owner, workerId),
                eq(effects.status, "leased"),
              ),
            )
            .returning({ id: effects.effect_id })
        ).length === 1
      );
    });
  const claimWhere = (now: number) =>
    and(
      lte(effects.available_at, now),
      or(
        inArray(effects.status, ["pending", "failed"]),
        and(eq(effects.status, "leased"), lte(effects.lease_expires_at, now)),
      ),
    );
  const claimRow = async (
    transaction: AnyPgDatabase,
    row: EffectRow | undefined,
    workerId: string,
    leaseMs: number,
    now: number,
  ) => {
    if (!row) return undefined;
    const attemptsCount = row.attempts + 1;
    const next: EffectRecord = {
      ...effectRecord(row),
      attempts: attemptsCount,
      leaseExpiresAt: now + leaseMs,
      leaseOwner: workerId,
      status: "leased",
      updatedAt: now,
    };
    const [updated] = await transaction
      .update(effects)
      .set({
        attempts: attemptsCount,
        data: encodedJsonb(next),
        lease_expires_at: now + leaseMs,
        lease_owner: workerId,
        status: "leased",
        updated_at: now,
      })
      .where(eq(effects.effect_id, row.effect_id))
      .returning();
    return updated ? effectRecord(updated) : undefined;
  };

  return {
    enqueue: (effect) =>
      db.transaction(async (transaction) => {
        const inserted = await transaction
          .insert(effects)
          .values({
            action_id: effect.actionId,
            attempts: effect.attempts,
            available_at: effect.availableAt,
            created_at: effect.createdAt,
            data: encodedJsonb(effect),
            effect_id: effect.effectId,
            handler: effect.handler,
            idempotency_key: effect.idempotencyKey,
            input_digest: effect.inputDigest,
            lease_expires_at: effect.leaseExpiresAt ?? null,
            lease_owner: effect.leaseOwner ?? null,
            run_id: effect.runId ?? null,
            status: effect.status,
            tenant_id: effect.tenantId,
            updated_at: effect.updatedAt,
          })
          .onConflictDoNothing()
          .returning({ id: effects.effect_id });
        if (inserted.length === 0) return false;
        await transaction.insert(outbox).values({
          created_at: effect.createdAt,
          effect_id: effect.effectId,
          event_id: `effect:${effect.effectId}`,
        });
        return true;
      }),
    claim: (workerId, leaseMs, now) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select()
          .from(effects)
          .where(claimWhere(now))
          .orderBy(asc(effects.available_at), asc(effects.created_at))
          .for("update", { skipLocked: true })
          .limit(1);
        return claimRow(transaction, row, workerId, leaseMs, now);
      }),
    claimEffect: (effectId, workerId, leaseMs, now) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select()
          .from(effects)
          .where(and(eq(effects.effect_id, effectId), claimWhere(now)))
          .for("update")
          .limit(1);
        return claimRow(transaction, row, workerId, leaseMs, now);
      }),
    heartbeat: async (effectId, workerId, leaseMs, now) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select()
          .from(effects)
          .where(
            and(
              eq(effects.effect_id, effectId),
              eq(effects.lease_owner, workerId),
              eq(effects.status, "leased"),
            ),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        const next = {
          ...effectRecord(row),
          leaseExpiresAt: now + leaseMs,
          updatedAt: now,
        };
        return (
          (
            await transaction
              .update(effects)
              .set({
                data: encodedJsonb(next),
                lease_expires_at: now + leaseMs,
                updated_at: now,
              })
              .where(eq(effects.effect_id, effectId))
              .returning({ id: effects.effect_id })
          ).length === 1
        );
      }),
    succeed: (effectId, workerId, result, now) =>
      updateLeased(effectId, workerId, { result, status: "succeeded" }, now),
    fail: (effectId, workerId, update, now) =>
      updateLeased(effectId, workerId, update, now),
    quarantineUnknown: (effectId, attempt, update, now) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select()
          .from(effects)
          .where(
            and(
              eq(effects.effect_id, effectId),
              eq(effects.attempts, attempt),
              eq(effects.status, "leased"),
            ),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        const next: EffectRecord = {
          ...withoutLease(effectRecord(row)),
          ...update,
          status: "unknown",
          updatedAt: now,
        };
        return (
          (
            await transaction
              .update(effects)
              .set({
                data: encodedJsonb(next),
                lease_expires_at: null,
                lease_owner: null,
                status: "unknown",
                updated_at: now,
              })
              .where(
                and(
                  eq(effects.effect_id, effectId),
                  eq(effects.attempts, attempt),
                  eq(effects.status, "leased"),
                ),
              )
              .returning({ id: effects.effect_id })
          ).length === 1
        );
      }),
    get: async (effectId) => {
      const [row] = await db
        .select()
        .from(effects)
        .where(eq(effects.effect_id, effectId))
        .limit(1);
      return row ? effectRecord(row) : undefined;
    },
    getByIdempotencyKey: async (tenantId, idempotencyKey) => {
      const [row] = await db
        .select()
        .from(effects)
        .where(
          and(
            eq(effects.tenant_id, tenantId),
            eq(effects.idempotency_key, idempotencyKey),
          ),
        )
        .limit(1);
      return row ? effectRecord(row) : undefined;
    },
    list: async (input) =>
      (
        await db
          .select()
          .from(effects)
          .where(
            and(
              input.tenantId
                ? eq(effects.tenant_id, input.tenantId)
                : undefined,
              input.runId ? eq(effects.run_id, input.runId) : undefined,
              input.status ? eq(effects.status, input.status) : undefined,
            ),
          )
          .orderBy(desc(effects.created_at))
          .limit(input.limit)
      ).map(effectRecord),
    listAttempts: async (effectId) =>
      (
        await db
          .select()
          .from(attempts)
          .where(eq(attempts.effect_id, effectId))
          .orderBy(attempts.started_at)
      ).map(attemptRecord),
    reconcile: (effectId, update, now) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select()
          .from(effects)
          .where(
            and(eq(effects.effect_id, effectId), eq(effects.status, "unknown")),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        const next = {
          ...effectRecord(row),
          ...update,
          updatedAt: now,
        };
        return (
          (
            await transaction
              .update(effects)
              .set({
                data: encodedJsonb(next),
                status: update.status,
                updated_at: now,
              })
              .where(
                and(
                  eq(effects.effect_id, effectId),
                  eq(effects.status, "unknown"),
                ),
              )
              .returning({ id: effects.effect_id })
          ).length === 1
        );
      }),
    claimOutbox: (workerId, leaseMs, now) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select()
          .from(outbox)
          .where(
            or(isNull(outbox.lease_owner), lte(outbox.lease_expires_at, now)),
          )
          .orderBy(outbox.created_at)
          .for("update", { skipLocked: true })
          .limit(1);
        if (!row) return undefined;
        const [updated] = await transaction
          .update(outbox)
          .set({
            attempts: row.attempts + 1,
            lease_expires_at: now + leaseMs,
            lease_owner: workerId,
          })
          .where(eq(outbox.event_id, row.event_id))
          .returning();
        return updated
          ? {
              attempts: updated.attempts,
              effectId: updated.effect_id,
              eventId: updated.event_id,
              leaseExpiresAt: updated.lease_expires_at!,
              leaseOwner: updated.lease_owner!,
            }
          : undefined;
      }),
    publishOutbox: async (eventId, workerId) =>
      (
        await db
          .delete(outbox)
          .where(
            and(eq(outbox.event_id, eventId), eq(outbox.lease_owner, workerId)),
          )
          .returning({ id: outbox.event_id })
      ).length === 1,
    retryOutbox: async (eventId, workerId) =>
      (
        await db
          .update(outbox)
          .set({ lease_expires_at: null, lease_owner: null })
          .where(
            and(eq(outbox.event_id, eventId), eq(outbox.lease_owner, workerId)),
          )
          .returning({ id: outbox.event_id })
      ).length === 1,
    recordAttempt: async (attempt) => {
      await db
        .insert(attempts)
        .values({
          attempt_id: attempt.attemptId,
          effect_id: attempt.effectId,
          error: attempt.error ?? null,
          finished_at: attempt.finishedAt ?? null,
          kind: attempt.kind,
          number: attempt.number,
          outcome: attempt.outcome,
          started_at: attempt.startedAt,
          worker_id: attempt.workerId,
        })
        .onConflictDoNothing();
    },
    finishAttempt: async (attemptId, outcome, now, error) => {
      await db
        .update(attempts)
        .set({ error: error ?? null, finished_at: now, outcome })
        .where(
          and(
            eq(attempts.attempt_id, attemptId),
            eq(attempts.outcome, "running"),
          ),
        );
    },
    startCompensation: (effectId, workerId, now) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select()
          .from(effects)
          .where(
            and(
              eq(effects.effect_id, effectId),
              inArray(effects.status, ["succeeded", "compensation_failed"]),
            ),
          )
          .for("update")
          .limit(1);
        if (!row) return undefined;
        const next: EffectRecord = {
          ...effectRecord(row),
          leaseOwner: workerId,
          status: "compensating",
          updatedAt: now,
        };
        const [updated] = await transaction
          .update(effects)
          .set({
            data: encodedJsonb(next),
            lease_owner: workerId,
            status: "compensating",
            updated_at: now,
          })
          .where(eq(effects.effect_id, effectId))
          .returning();
        return updated ? effectRecord(updated) : undefined;
      }),
    finishCompensation: (effectId, workerId, now, error) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select()
          .from(effects)
          .where(
            and(
              eq(effects.effect_id, effectId),
              eq(effects.lease_owner, workerId),
              eq(effects.status, "compensating"),
            ),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        const next: EffectRecord = {
          ...withoutLease(effectRecord(row)),
          ...(error === undefined ? {} : { error }),
          status: error === undefined ? "compensated" : "compensation_failed",
          updatedAt: now,
        };
        return (
          (
            await transaction
              .update(effects)
              .set({
                data: encodedJsonb(next),
                lease_expires_at: null,
                lease_owner: null,
                status: next.status,
                updated_at: now,
              })
              .where(eq(effects.effect_id, effectId))
              .returning({ id: effects.effect_id })
          ).length === 1
        );
      }),
    listReconciliations: async (effectId) =>
      (
        await db
          .select()
          .from(reconciliations)
          .where(eq(reconciliations.effect_id, effectId))
          .orderBy(
            asc(reconciliations.created_at),
            asc(reconciliations.reconciliation_id),
          )
      ).map(reconciliationRecord),
    resolveUnknown: ({
      effectId,
      reconciliation,
      status,
      updatedAt,
      ...update
    }) =>
      db.transaction(async (transaction) => {
        const [row] = await transaction
          .select()
          .from(effects)
          .where(
            and(
              eq(effects.effect_id, effectId),
              eq(effects.tenant_id, reconciliation.tenantId),
              eq(effects.status, "unknown"),
            ),
          )
          .for("update")
          .limit(1);
        if (!row) return false;
        const base = effectRecord(row);
        const next: EffectRecord = {
          ...base,
          ...update,
          status,
          updatedAt,
        };
        if (update.error === undefined) delete next.error;
        if (update.result === undefined) delete next.result;
        await transaction
          .update(effects)
          .set({
            data: encodedJsonb(next),
            status,
            updated_at: updatedAt,
          })
          .where(eq(effects.effect_id, effectId));
        await transaction.insert(reconciliations).values({
          actor_id: reconciliation.actorId,
          created_at: updatedAt,
          effect_id: effectId,
          evidence_reference: reconciliation.evidenceReference,
          note: reconciliation.note,
          reconciliation_id: reconciliation.reconciliationId,
          resolution: reconciliation.resolution,
          source: reconciliation.source,
          tenant_id: reconciliation.tenantId,
        });
        if (status === "pending")
          await transaction.insert(outbox).values({
            created_at: updatedAt,
            effect_id: effectId,
            event_id: `effect:${effectId}:recovery:${reconciliation.reconciliationId}`,
          });
        return true;
      }),
  };
};
