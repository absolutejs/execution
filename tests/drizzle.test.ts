import { SQL } from "bun";
import { expect, test } from "bun:test";
import { sql as expression } from "drizzle-orm";
import { drizzle } from "drizzle-orm/bun-sql";
import {
  createDrizzleEffectAdapterHealthStore,
  createDrizzleEffectAdapterInstallationStore,
  createDrizzleEffectAdapterRegistryStore,
  createDrizzleEffectEvidenceStore,
  createDrizzleEffectReconciliationLeaseStore,
  createDrizzleEffectReconciliationSchedulerStore,
  createDrizzleEffectStore,
  type EffectAdapterDescriptor,
  type EffectRecord,
} from "../src";

const databaseUrl = process.env.DATABASE_URL;
const descriptor = (id: string): EffectAdapterDescriptor => ({
  adapterId: id,
  compensation: { supported: false },
  credentialBindings: [],
  destinations: [],
  effects: ["simulation.complete"],
  idempotency: { scope: "tenant-effect", supported: true },
  reconciliation: { mode: "manual" },
  spendAuthority: {
    canSpend: false,
    currencies: [],
    requiresMandate: false,
  },
  title: "Drizzle simulation",
  version: "1.0.0",
});

test.skipIf(!databaseUrl)(
  "Drizzle stores preserve atomic execution and native JSONB",
  async () => {
    const client = new SQL({ max: 4, prepare: false, url: databaseUrl! });
    const db = drizzle({ client });
    const rollback = new Error("expected rollback");
    try {
      await db.transaction(async (transaction) => {
        const suffix = crypto.randomUUID();
        const tenantId = `tenant-${suffix}`;
        const effectId = `effect-${suffix}`;
        const effect: EffectRecord = {
          actionId: `action-${suffix}`,
          attempts: 0,
          availableAt: 0,
          createdAt: 0,
          effectId,
          handler: "simulation.complete",
          idempotencyKey: `key-${suffix}`,
          input: { native: true },
          inputDigest: `digest-${suffix}`,
          status: "pending",
          tenantId,
          updatedAt: 0,
        };
        const effects = createDrizzleEffectStore(transaction);
        expect(await effects.enqueue(effect)).toBe(true);
        expect(await effects.enqueue(effect)).toBe(false);
        const claimed = await effects.claimEffect(effectId, "worker", 100, 1);
        expect(claimed).toMatchObject({
          attempts: 1,
          leaseOwner: "worker",
          status: "leased",
        });
        expect(
          await effects.claimEffect(effectId, "other-worker", 100, 1),
        ).toBeUndefined();
        expect(
          await effects.quarantineUnknown(
            effectId,
            1,
            { error: "provider outcome unknown" },
            2,
          ),
        ).toBe(true);
        expect(
          await effects.resolveUnknown({
            effectId,
            reconciliation: {
              actorId: "operator",
              createdAt: 3,
              effectId,
              evidenceReference: `evidence-${suffix}`,
              note: "verified retry",
              reconciliationId: `reconciliation-${suffix}`,
              resolution: "confirmed_not_applied",
              source: "operator",
              tenantId,
            },
            status: "pending",
            updatedAt: 3,
          }),
        ).toBe(true);
        expect(await effects.listReconciliations(effectId)).toHaveLength(1);

        const registry = createDrizzleEffectAdapterRegistryStore(transaction);
        const adapter = descriptor(`adapter-${suffix}`);
        await registry.save({
          active: false,
          descriptor: adapter,
          descriptorDigest: `descriptor-${suffix}`,
          registeredAt: 1,
          updatedAt: 1,
        });
        expect((await registry.get(adapter.adapterId))?.descriptor).toEqual(
          adapter,
        );

        const installations =
          createDrizzleEffectAdapterInstallationStore(transaction);
        const installationId = `installation-${suffix}`;
        await installations.save({
          adapterId: adapter.adapterId,
          adapterVersion: adapter.version,
          descriptorDigest: `descriptor-${suffix}`,
          enabled: false,
          installationId,
          installedAt: 1,
          policy: {
            credentials: [],
            destinations: [],
            effects: ["simulation.complete"],
            spend: {
              currency: null,
              mandateId: null,
              maxMinorPerEffect: 0,
            },
          },
          tenantId,
          updatedAt: 1,
        });
        expect(
          (await installations.get(tenantId, installationId))?.tenantId,
        ).toBe(tenantId);

        const evidence = createDrizzleEffectEvidenceStore(transaction);
        await evidence.put({
          deliveryId: `delivery-${suffix}`,
          effectId,
          eventType: "verified",
          evidenceReference: `evidence-${suffix}`,
          occurredAt: 4,
          outcome: "confirmed_not_applied",
          provider: "simulation",
          receivedAt: 4,
          tenantId,
          verifier: "test",
        });
        expect(
          await evidence.list({ effectId, limit: 10, tenantId }),
        ).toHaveLength(1);

        const health = createDrizzleEffectAdapterHealthStore(transaction);
        const healthInput = {
          adapterId: adapter.adapterId,
          checkedAt: 5,
          code: "query_ok",
          provider: "simulation",
          scopeId: installationId,
          signal: "provider-query" as const,
          status: "healthy" as const,
          tenantId,
        };
        await health.observe(healthInput);
        expect(
          (await health.observe({ ...healthInput, checkedAt: 6 })).successes,
        ).toBe(2);

        const leases = createDrizzleEffectReconciliationLeaseStore(transaction);
        expect(
          await leases.claim({
            effectId,
            leaseMs: 100,
            now: 7,
            owner: "reconciler",
          }),
        ).toBe(true);
        expect(
          await leases.complete({
            effectId,
            nextCheckAt: 200,
            now: 8,
            owner: "reconciler",
          }),
        ).toBe(true);

        const scheduler =
          createDrizzleEffectReconciliationSchedulerStore(transaction);
        const schedulerId = `scheduler-${suffix}`;
        await scheduler.initialize({
          enabled: true,
          intervalMs: 100,
          now: 9,
          schedulerId,
        });
        expect(
          await scheduler.claim({
            leaseMs: 100,
            now: 9,
            owner: "scheduler-worker",
            schedulerId,
          }),
        ).toBe(true);
        expect(
          await scheduler.complete({
            now: 10,
            owner: "scheduler-worker",
            result: {
              failed: 0,
              pending: 1,
              resolved: 0,
              scanned: 1,
              skipped: 0,
            },
            schedulerId,
          }),
        ).toBe(true);
        expect((await scheduler.read(schedulerId))?.lastResult).toEqual({
          failed: 0,
          pending: 1,
          resolved: 0,
          scanned: 1,
          skipped: 0,
        });

        const shapes = await transaction.execute(
          expression<{
            descriptor: string;
            effect: string;
            policy: string;
          }>`
            select jsonb_typeof(data) as effect,
                   (select jsonb_typeof(descriptor) from execution.adapter_registry where adapter_id = ${adapter.adapterId}) as descriptor,
                   (select jsonb_typeof(policy) from execution.adapter_installations where installation_id = ${installationId}) as policy
            from execution.effects where effect_id = ${effectId}
          `,
        );
        expect(shapes[0]).toMatchObject({
          descriptor: "object",
          effect: "object",
          policy: "object",
        });
        throw rollback;
      });
    } catch (error) {
      if (error !== rollback) throw error;
    } finally {
      await client.close();
    }
  },
);
