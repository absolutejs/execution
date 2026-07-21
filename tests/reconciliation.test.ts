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
      requiresReference: false,
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
if (descriptor.reconciliation.mode !== "query")
  throw new Error("Expected query descriptor");
const hybridDescriptor: EffectAdapterDescriptor = {
  ...descriptor,
  reconciliation: {
    mode: "webhook-query",
    query: {
      ...descriptor.reconciliation.query,
      requiresReference: true,
    },
    webhook: {
      callback: {
        body: "raw",
        mediaType: "application/json",
        method: "POST",
        pathTemplate: "/webhooks/{tenantId}/provider",
        signatureHeaders: ["provider-signature"],
      },
      events: ["delivery.confirmed"],
      health: { strategy: "last-verified-event" },
      provider: "provider",
      secret: {
        alias: "PROVIDER_WEBHOOK_SECRET",
        rotation: { mode: "replace", verification: "signed-event" },
      },
    },
  },
  version: "1.1.0",
};

const setup = (options?: {
  descriptor?: EffectAdapterDescriptor;
  effect?: EffectRecord;
  fail?: boolean;
}) => {
  const configuredDescriptor = options?.descriptor ?? descriptor;
  const configuredEffect = options?.effect ?? effect;
  const events: string[] = [];
  const healthStore = createMemoryEffectAdapterHealthStore();
  const evidence: unknown[] = [];
  let queries = 0;
  const listInputs: unknown[] = [];
  const runtime = createEffectAdapterReconciliationRuntime({
    drivers: [
      {
        adapterId: configuredDescriptor.adapterId,
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
        version: configuredDescriptor.version,
      },
    ],
    effects: {
      list: async (input) => {
        listInputs.push(input);
        return [configuredEffect];
      },
    },
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
          adapter: configuredDescriptor,
          credentials: [
            {
              adapterAlias: "API_TOKEN",
              destination: "https://api.example.test",
              secretAlias: "PROJECT_PROVIDER_TOKEN",
            },
          ],
          installation: {
            adapterId: configuredDescriptor.adapterId,
            adapterVersion: configuredDescriptor.version,
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
  return {
    evidence,
    events,
    healthStore,
    listInputs,
    queries: () => queries,
    runtime,
  };
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

  test("passes an exact tenant fence into inventory before querying", async () => {
    const harness = setup();
    await harness.runtime.runOnce({ tenantId: "tenant-1" });
    expect(harness.listInputs).toEqual([
      { limit: 10, status: "unknown", tenantId: "tenant-1" },
    ]);
    expect(harness.queries()).toBe(1);
    await expect(harness.runtime.runOnce({ tenantId: "   " })).rejects.toThrow(
      "tenantId is required",
    );
    expect(harness.queries()).toBe(1);
  });

  test("queries a webhook fallback only when its exact provider reference exists", async () => {
    const withoutReference = setup({ descriptor: hybridDescriptor });
    expect(await withoutReference.runtime.runOnce()).toEqual({
      failed: 0,
      pending: 0,
      resolved: 0,
      scanned: 1,
      skipped: 1,
    });
    expect(withoutReference.events).toEqual(["authorize"]);
    expect(withoutReference.queries()).toBe(0);

    const withReference = setup({
      descriptor: hybridDescriptor,
      effect: {
        ...effect,
        reconciliationReference: {
          adapterId: hybridDescriptor.adapterId,
          provider: "provider",
          resourceId: "provider-resource-1",
        },
      },
    });
    expect((await withReference.runtime.runOnce()).resolved).toBe(1);
    expect(withReference.events).toContain(
      "query:effectId,idempotencyKey,inputDigest,reconciliationReference",
    );
    expect(withReference.queries()).toBe(1);
  });

  test("rejects a reconciliation reference rebound to another adapter", async () => {
    const harness = setup({
      descriptor: hybridDescriptor,
      effect: {
        ...effect,
        reconciliationReference: {
          adapterId: "other.adapter",
          provider: "provider",
          resourceId: "provider-resource-1",
        },
      },
    });
    expect((await harness.runtime.runOnce()).failed).toBe(1);
    expect(harness.events).toEqual(["authorize"]);
    expect(harness.queries()).toBe(0);
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
