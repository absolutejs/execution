import {
  and,
  asc,
  desc,
  eq,
  isNull,
  lte,
  or,
  sql,
  type SQL,
} from "drizzle-orm";
import type { PgAsyncDatabase } from "drizzle-orm/pg-core";
import {
  EffectAdapterInstallationError,
  type EffectAdapterInstallationRecord,
  type EffectAdapterInstallationStore,
} from "./adapterInstallations";
import type {
  EffectAdapterRegistryRecord,
  EffectAdapterRegistryStore,
} from "./adapterRegistry";
import type { EffectEvidenceRecord, EffectEvidenceStore } from "./evidence";
import {
  EffectAdapterReconciliationError,
  type EffectAdapterHealthObservation,
  type EffectAdapterHealthRecord,
  type EffectAdapterHealthStore,
  type EffectReconciliationLeaseStore,
} from "./reconciliation";
import {
  EffectReconciliationSchedulerError,
  type EffectReconciliationSchedulerRecord,
  type EffectReconciliationSchedulerStore,
} from "./scheduler";
import { encodedJsonb, executionDrizzleSchema } from "./drizzleSchema";

type AnyPgDatabase = PgAsyncDatabase<any, any>;
type Tables = ReturnType<typeof executionDrizzleSchema>;

const registryRecord = (
  row: Tables["adapterRegistry"]["$inferSelect"],
): EffectAdapterRegistryRecord => ({
  active: row.active,
  ...(row.certification ? { certification: row.certification } : {}),
  descriptor: row.descriptor,
  descriptorDigest: row.descriptor_digest,
  registeredAt: row.registered_at,
  updatedAt: row.updated_at,
});
const installationRecord = (
  row: Tables["installations"]["$inferSelect"],
): EffectAdapterInstallationRecord => ({
  adapterId: row.adapter_id,
  adapterVersion: row.adapter_version,
  descriptorDigest: row.descriptor_digest,
  enabled: row.enabled,
  installationId: row.installation_id,
  installedAt: row.installed_at,
  policy: row.policy,
  tenantId: row.tenant_id,
  updatedAt: row.updated_at,
});
const evidenceRecord = (
  row: Tables["evidence"]["$inferSelect"],
): EffectEvidenceRecord => ({
  deliveryId: row.delivery_id,
  effectId: row.effect_id,
  eventType: row.event_type,
  evidenceReference: row.evidence_reference,
  occurredAt: row.occurred_at,
  outcome: row.outcome as EffectEvidenceRecord["outcome"],
  provider: row.provider,
  ...(row.provider_resource_id
    ? { providerResourceId: row.provider_resource_id }
    : {}),
  receivedAt: row.received_at,
  tenantId: row.tenant_id,
  verifier: row.verifier,
});
const healthRecord = (
  row: Tables["health"]["$inferSelect"],
): EffectAdapterHealthRecord => ({
  adapterId: row.adapter_id,
  checkedAt: row.checked_at,
  code: row.code,
  failures: row.failures,
  ...(row.last_failure_at === null
    ? {}
    : { lastFailureAt: row.last_failure_at }),
  ...(row.last_success_at === null
    ? {}
    : { lastSuccessAt: row.last_success_at }),
  provider: row.provider,
  scopeId: row.scope_id,
  signal: row.signal as EffectAdapterHealthRecord["signal"],
  status: row.status as EffectAdapterHealthRecord["status"],
  successes: row.successes,
  tenantId: row.tenant_id,
});
const schedulerRecord = (
  row: Tables["scheduler"]["$inferSelect"],
): EffectReconciliationSchedulerRecord => ({
  enabled: row.enabled,
  intervalMs: row.interval_ms,
  ...(row.last_completed_at === null
    ? {}
    : { lastCompletedAt: row.last_completed_at }),
  ...(row.last_error_code === null
    ? {}
    : { lastErrorCode: row.last_error_code }),
  ...(row.last_scanned === null ||
  row.last_pending === null ||
  row.last_resolved === null ||
  row.last_failed === null ||
  row.last_skipped === null
    ? {}
    : {
        lastResult: {
          failed: row.last_failed,
          pending: row.last_pending,
          resolved: row.last_resolved,
          scanned: row.last_scanned,
          skipped: row.last_skipped,
        },
      }),
  ...(row.last_started_at === null
    ? {}
    : { lastStartedAt: row.last_started_at }),
  nextRunAt: row.next_run_at,
  schedulerId: row.scheduler_id,
  updatedAt: row.updated_at,
});

export const createDrizzleEffectAdapterRegistryStore = <
  DB extends AnyPgDatabase,
>(
  db: DB,
  options: { namespace?: string } = {},
): EffectAdapterRegistryStore => {
  const { adapterRegistry } = executionDrizzleSchema(options.namespace);
  return {
    get: async (adapterId) => {
      const [row] = await db
        .select()
        .from(adapterRegistry)
        .where(eq(adapterRegistry.adapter_id, adapterId))
        .limit(1);
      return row ? registryRecord(row) : undefined;
    },
    list: async () =>
      (
        await db
          .select()
          .from(adapterRegistry)
          .orderBy(adapterRegistry.adapter_id)
      ).map(registryRecord),
    save: async (record) => {
      await db
        .insert(adapterRegistry)
        .values({
          active: record.active,
          adapter_id: record.descriptor.adapterId,
          certification: record.certification
            ? encodedJsonb(record.certification)
            : null,
          descriptor: encodedJsonb(record.descriptor),
          descriptor_digest: record.descriptorDigest,
          registered_at: record.registeredAt,
          updated_at: record.updatedAt,
          version: record.descriptor.version,
        })
        .onConflictDoUpdate({
          set: {
            active: record.active,
            certification: record.certification
              ? encodedJsonb(record.certification)
              : null,
            descriptor: encodedJsonb(record.descriptor),
            descriptor_digest: record.descriptorDigest,
            registered_at: record.registeredAt,
            updated_at: record.updatedAt,
            version: record.descriptor.version,
          },
          target: adapterRegistry.adapter_id,
        });
    },
  };
};

export const createDrizzleEffectAdapterInstallationStore = <
  DB extends AnyPgDatabase,
>(
  db: DB,
  options: { namespace?: string } = {},
): EffectAdapterInstallationStore => {
  const { installations } = executionDrizzleSchema(options.namespace);
  return {
    get: async (tenantId, installationId) => {
      const [row] = await db
        .select()
        .from(installations)
        .where(
          and(
            eq(installations.tenant_id, tenantId),
            eq(installations.installation_id, installationId),
          ),
        )
        .limit(1);
      return row ? installationRecord(row) : undefined;
    },
    list: async (input) =>
      (
        await db
          .select()
          .from(installations)
          .where(
            input?.tenantId
              ? eq(installations.tenant_id, input.tenantId)
              : undefined,
          )
          .orderBy(
            installations.tenant_id,
            installations.adapter_id,
            installations.installation_id,
          )
      ).map(installationRecord),
    save: async (record) => {
      const rows = await db
        .insert(installations)
        .values({
          adapter_id: record.adapterId,
          adapter_version: record.adapterVersion,
          descriptor_digest: record.descriptorDigest,
          enabled: record.enabled,
          installation_id: record.installationId,
          installed_at: record.installedAt,
          policy: encodedJsonb(record.policy),
          tenant_id: record.tenantId,
          updated_at: record.updatedAt,
        })
        .onConflictDoUpdate({
          set: {
            adapter_id: record.adapterId,
            adapter_version: record.adapterVersion,
            descriptor_digest: record.descriptorDigest,
            enabled: record.enabled,
            installed_at: record.installedAt,
            policy: encodedJsonb(record.policy),
            updated_at: record.updatedAt,
          },
          setWhere: eq(installations.tenant_id, record.tenantId),
          target: installations.installation_id,
        })
        .returning({ id: installations.installation_id });
      if (rows.length !== 1)
        throw new EffectAdapterInstallationError(
          "Installation identity belongs to another tenant",
        );
    },
  };
};

export const createDrizzleEffectEvidenceStore = <DB extends AnyPgDatabase>(
  db: DB,
  options: { namespace?: string } = {},
): EffectEvidenceStore => {
  const { evidence } = executionDrizzleSchema(options.namespace);
  return {
    get: async (provider, deliveryId) => {
      const [row] = await db
        .select()
        .from(evidence)
        .where(
          and(
            eq(evidence.provider, provider),
            eq(evidence.delivery_id, deliveryId),
          ),
        )
        .limit(1);
      return row ? evidenceRecord(row) : undefined;
    },
    list: async ({ effectId, limit, tenantId }) =>
      (
        await db
          .select()
          .from(evidence)
          .where(
            and(
              tenantId ? eq(evidence.tenant_id, tenantId) : undefined,
              effectId ? eq(evidence.effect_id, effectId) : undefined,
            ),
          )
          .orderBy(desc(evidence.received_at))
          .limit(limit)
      ).map(evidenceRecord),
    put: async (record) =>
      (
        await db
          .insert(evidence)
          .values({
            delivery_id: record.deliveryId,
            effect_id: record.effectId,
            event_type: record.eventType,
            evidence_reference: record.evidenceReference,
            occurred_at: record.occurredAt,
            outcome: record.outcome,
            provider: record.provider,
            provider_resource_id: record.providerResourceId ?? null,
            received_at: record.receivedAt,
            tenant_id: record.tenantId,
            verifier: record.verifier,
          })
          .onConflictDoNothing()
          .returning({ id: evidence.delivery_id })
      ).length === 1,
  };
};

export const createDrizzleEffectAdapterHealthStore = <DB extends AnyPgDatabase>(
  db: DB,
  options: { namespace?: string } = {},
): EffectAdapterHealthStore => {
  const { health } = executionDrizzleSchema(options.namespace);
  return {
    list: async ({ adapterId, limit, tenantId }) =>
      (
        await db
          .select()
          .from(health)
          .where(
            and(
              tenantId ? eq(health.tenant_id, tenantId) : undefined,
              adapterId ? eq(health.adapter_id, adapterId) : undefined,
            ),
          )
          .orderBy(desc(health.checked_at))
          .limit(limit)
      ).map(healthRecord),
    observe: async (
      observation: EffectAdapterHealthObservation & { checkedAt: number },
    ) => {
      const success = observation.status === "healthy";
      const [row] = await db
        .insert(health)
        .values({
          adapter_id: observation.adapterId,
          checked_at: observation.checkedAt,
          code: observation.code,
          failures: success ? 0 : 1,
          last_failure_at: success ? null : observation.checkedAt,
          last_success_at: success ? observation.checkedAt : null,
          provider: observation.provider,
          scope_id: observation.scopeId,
          signal: observation.signal,
          status: observation.status,
          successes: success ? 1 : 0,
          tenant_id: observation.tenantId,
        })
        .onConflictDoUpdate({
          set: {
            checked_at: observation.checkedAt,
            code: observation.code,
            failures: sql`${health.failures} + ${success ? 0 : 1}`,
            last_failure_at: success
              ? sql`${health.last_failure_at}`
              : observation.checkedAt,
            last_success_at: success
              ? observation.checkedAt
              : sql`${health.last_success_at}`,
            provider: observation.provider,
            status: observation.status,
            successes: sql`${health.successes} + ${success ? 1 : 0}`,
          },
          target: [
            health.adapter_id,
            health.tenant_id,
            health.signal,
            health.scope_id,
          ],
        })
        .returning();
      if (!row)
        throw new EffectAdapterReconciliationError(
          "Reconciliation health observation was not retained",
        );
      return healthRecord(row);
    },
  };
};

export const createDrizzleEffectReconciliationLeaseStore = <
  DB extends AnyPgDatabase,
>(
  db: DB,
  options: { namespace?: string } = {},
): EffectReconciliationLeaseStore => {
  const { querySchedule } = executionDrizzleSchema(options.namespace);
  return {
    claim: async ({ effectId, leaseMs, now, owner }) =>
      (
        await db
          .insert(querySchedule)
          .values({
            attempts: 1,
            effect_id: effectId,
            lease_expires_at: now + leaseMs,
            lease_owner: owner,
            next_check_at: 0,
            updated_at: now,
          })
          .onConflictDoUpdate({
            set: {
              attempts: sql`${querySchedule.attempts} + 1`,
              lease_expires_at: now + leaseMs,
              lease_owner: owner,
              updated_at: now,
            },
            setWhere: and(
              lte(querySchedule.next_check_at, now),
              or(
                isNull(querySchedule.lease_expires_at),
                lte(querySchedule.lease_expires_at, now),
              ),
            ),
            target: querySchedule.effect_id,
          })
          .returning({ id: querySchedule.effect_id })
      ).length === 1,
    complete: async ({ effectId, errorCode, nextCheckAt, now, owner }) =>
      (
        await db
          .update(querySchedule)
          .set({
            last_error_code: errorCode ?? null,
            lease_expires_at: null,
            lease_owner: null,
            next_check_at: nextCheckAt,
            updated_at: now,
          })
          .where(
            and(
              eq(querySchedule.effect_id, effectId),
              eq(querySchedule.lease_owner, owner),
            ),
          )
          .returning({ id: querySchedule.effect_id })
      ).length === 1,
  };
};

export const createDrizzleEffectReconciliationSchedulerStore = <
  DB extends AnyPgDatabase,
>(
  db: DB,
  options: { namespace?: string } = {},
): EffectReconciliationSchedulerStore => {
  const { scheduler } = executionDrizzleSchema(options.namespace);
  const one = async (
    query: Promise<Array<Tables["scheduler"]["$inferSelect"]>>,
  ) => {
    const row = (await query)[0];
    if (!row)
      throw new EffectReconciliationSchedulerError(
        "Reconciliation scheduler record is unavailable",
      );
    return schedulerRecord(row);
  };
  return {
    claim: async ({ leaseMs, now, owner, schedulerId }) =>
      (
        await db
          .update(scheduler)
          .set({
            last_started_at: now,
            lease_expires_at: now + leaseMs,
            lease_owner: owner,
            updated_at: now,
          })
          .where(
            and(
              eq(scheduler.scheduler_id, schedulerId),
              eq(scheduler.enabled, true),
              lte(scheduler.next_run_at, now),
              or(
                isNull(scheduler.lease_expires_at),
                lte(scheduler.lease_expires_at, now),
              ),
            ),
          )
          .returning({ id: scheduler.scheduler_id })
      ).length === 1,
    complete: async ({ errorCode, now, owner, result, schedulerId }) =>
      (
        await db
          .update(scheduler)
          .set({
            last_completed_at: now,
            last_error_code: errorCode ?? null,
            last_failed: result?.failed ?? null,
            last_pending: result?.pending ?? null,
            last_resolved: result?.resolved ?? null,
            last_scanned: result?.scanned ?? null,
            last_skipped: result?.skipped ?? null,
            lease_expires_at: null,
            lease_owner: null,
            next_run_at: sql`${now} + ${scheduler.interval_ms}`,
            updated_at: now,
          })
          .where(
            and(
              eq(scheduler.scheduler_id, schedulerId),
              eq(scheduler.lease_owner, owner),
            ),
          )
          .returning({ id: scheduler.scheduler_id })
      ).length === 1,
    configure: ({ enabled, intervalMs, now, schedulerId }) =>
      one(
        db
          .insert(scheduler)
          .values({
            enabled,
            interval_ms: intervalMs,
            next_run_at: now,
            scheduler_id: schedulerId,
            updated_at: now,
          })
          .onConflictDoUpdate({
            set: {
              enabled,
              interval_ms: intervalMs,
              next_run_at: sql`case when ${enabled} and not ${scheduler.enabled} then ${now} else ${scheduler.next_run_at} end`,
              updated_at: now,
            },
            target: scheduler.scheduler_id,
          })
          .returning(),
      ),
    initialize: ({ enabled, intervalMs, now, schedulerId }) =>
      one(
        db
          .insert(scheduler)
          .values({
            enabled,
            interval_ms: intervalMs,
            next_run_at: now,
            scheduler_id: schedulerId,
            updated_at: now,
          })
          .onConflictDoUpdate({
            set: { scheduler_id: schedulerId },
            target: scheduler.scheduler_id,
          })
          .returning(),
      ),
    read: async (schedulerId) => {
      const [row] = await db
        .select()
        .from(scheduler)
        .where(eq(scheduler.scheduler_id, schedulerId))
        .limit(1);
      return row ? schedulerRecord(row) : undefined;
    },
  };
};

export { executionDrizzleSchema } from "./drizzleSchema";
