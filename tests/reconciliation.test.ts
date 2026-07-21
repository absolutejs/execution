import { describe, expect, test } from "bun:test";
import {
  createEffectAdapterHealthOperations,
  createEffectAdapterReconciliationRuntime,
  createMemoryEffectAdapterHealthStore,
  createMemoryEffectReconciliationLeaseStore,
  effectAdapterReconciliationPostgresSchemaSql,
  type EffectAdapterDescriptor,
  type EffectRecord,
} from "../src";

const NOW = 1_721_563_200_000;
const descriptor: EffectAdapterDescriptor = {
  adapterId: "provider",
  compensation: { supported: false },
  credentialBindings: [
    {
      alias: "API_TOKEN",
      destination: "https://api.example.test",
      mode: "http-header",
    },
  ],
  destinations: [{ kind: "https-origin", value: "https://api.example.test" }],
  effects: ["message.send"],
  idempotency: { scope: "tenant-effect", supported: true },
  reconciliation: {
    mode: "query",
    query: {
      credentialAlias: "API_TOKEN",
      health: {
        staleAfterMs: 900_000,
        strategy: "last-successful-query",
      },
      pollingIntervalMs: 60_000,
      provider: "provider",
      rotation: { mode: "replace", verification: "successful-query" },
      supportedOutcomes: ["confirmed_succeeded", "confirmed_not_applied"],
    },
  },
  spendAuthority: { canSpend: false, currencies: [], requiresMandate: false },
  title: "Provider",
  version: "1.0.0",
};
const effect: EffectRecord = {
  actionId: "action-1",
  attempts: 1,
  availableAt: NOW,
  createdAt: NOW,
  effectId: "effect-1",
  handler: "installed-adapter",
  idempotencyKey: "message-1",
  input: {
    destination: "https://api.example.test",
    effect: "message.send",
    installationId: "installation-1",
    payload: { private: "never-query-this" },
  },
  inputDigest: "sha256:input",
  status: "unknown",
  tenantId: "tenant-1",
  updatedAt: NOW,
};

const setup = (options?: { fail?: boolean }) => {
  const events: string[] = [];
  const healthStore = createMemoryEffectAdapterHealthStore();
  const evidence: unknown[] = [];
  let queries = 0;
  const runtime = createEffectAdapterReconciliationRuntime({
    drivers: [
      {
        adapterId: descriptor.adapterId,
        provider: "provider",
        query: async (queryEffect) => {
          queries += 1;
          events.push(`query:${Object.keys(queryEffect).sort().join(",")}`);
          if (options?.fail)
            throw new Error("secret-value raw provider failure");
          return {
            deliveryId: "delivery-1",
            eventType: "delivery.confirmed",
            evidenceReference: "provider:delivery-1",
            occurredAt: NOW,
            outcome: "confirmed_succeeded",
            status: "resolved",
            verifier: "provider-query-v1",
          };
        },
        version: descriptor.version,
      },
    ],
    effects: { list: async () => [effect] },
    health: createEffectAdapterHealthOperations({
      now: () => NOW,
      store: healthStore,
    }),
    ingestEvidence: async (record, source) => {
      evidence.push({ record, source });
    },
    installations: {
      authorize: async () => {
        events.push("authorize");
        return {
          adapter: descriptor,
          credentials: [
            {
              adapterAlias: "API_TOKEN",
              destination: "https://api.example.test",
              secretAlias: "PROJECT_PROVIDER_TOKEN",
            },
          ],
          installation: {
            adapterId: descriptor.adapterId,
            adapterVersion: descriptor.version,
            descriptorDigest: "sha256:descriptor",
            enabled: true,
            installationId: "installation-1",
            installedAt: NOW,
            policy: {
              credentials: [],
              destinations: ["https://api.example.test"],
              effects: ["message.send"],
              spend: {
                currency: null,
                mandateId: null,
                maxMinorPerEffect: 0,
              },
            },
            tenantId: "tenant-1",
            updatedAt: NOW,
          },
        };
      },
    },
    leaseMs: 30_000,
    leases: createMemoryEffectReconciliationLeaseStore(),
    limit: 10,
    now: () => NOW,
    resolveCredential: async () => {
      events.push("credential");
      return "secret-value";
    },
    workerId: "worker-1",
  });
  return { evidence, events, healthStore, queries: () => queries, runtime };
};

describe("effect adapter reconciliation runtime", () => {
  test("authorizes before credentials and retains only normalized query evidence", async () => {
    const harness = setup();
    expect(await harness.runtime.runOnce()).toEqual({
      failed: 0,
      pending: 0,
      resolved: 1,
      scanned: 1,
      skipped: 0,
    });
    expect(harness.events).toEqual([
      "authorize",
      "credential",
      "query:effectId,idempotencyKey,inputDigest",
    ]);
    expect(JSON.stringify(harness.evidence)).not.toContain("never-query-this");
    expect(JSON.stringify(harness.evidence)).not.toContain("secret-value");
    expect(harness.evidence).toHaveLength(1);
  });

  test("a shared lease prevents replicas from querying one effect twice", async () => {
    const harness = setup();
    await Promise.all([harness.runtime.runOnce(), harness.runtime.runOnce()]);
    expect(harness.queries()).toBe(1);
  });

  test("provider failures retain a bounded safe code, never raw errors", async () => {
    const harness = setup({ fail: true });
    expect((await harness.runtime.runOnce()).failed).toBe(1);
    const health = await harness.healthStore.list({ limit: 10 });
    expect(health[0]?.code).toBe("query_failed");
    expect(JSON.stringify(health)).not.toContain("secret-value");
    expect(JSON.stringify(health)).not.toContain("raw provider failure");
  });

  test("ships bounded health and replica-safe query schedule schema", () => {
    const sql = effectAdapterReconciliationPostgresSchemaSql();
    expect(sql).toContain(
      "PRIMARY KEY (adapter_id, tenant_id, signal, scope_id)",
    );
    expect(sql).toContain("effect_reconciliation_query_schedule");
    expect(sql).toContain("lease_expires_at");
  });
});
