import { describe, expect, test } from "bun:test";
import {
  createEffectEvidenceIngestion,
  createMemoryEffectEvidenceStore,
  effectEvidencePostgresSchemaSql,
  EffectEvidenceError,
  type EffectEvidenceRecord,
} from "../src";

const evidence = (effectId = "effect-a"): EffectEvidenceRecord => ({
  deliveryId: "svix-a",
  effectId,
  eventType: "email.sent",
  evidenceReference: "resend:webhook:svix-a",
  occurredAt: 1,
  outcome: "confirmed_succeeded",
  provider: "resend",
  providerResourceId: "email-a",
  receivedAt: 2,
  tenantId: "tenant-a",
  verifier: "resend-sdk",
});

describe("effect evidence ingestion", () => {
  test("deduplicates delivery while resuming reconciliation", async () => {
    const store = createMemoryEffectEvidenceStore();
    let reconciliations = 0;
    const ingestion = createEffectEvidenceIngestion({
      authorize: async () => true,
      reconcile: async () => {
        reconciliations += 1;
        return reconciliations === 1 ? "resolved" : "already_terminal";
      },
      settle: async () => {},
      store,
    });
    expect((await ingestion.ingest(evidence())).duplicate).toBe(false);
    expect((await ingestion.ingest(evidence())).duplicate).toBe(true);
    expect(reconciliations).toBe(2);
    expect(await store.list({ limit: 10 })).toHaveLength(1);
  });

  test("rejects a duplicate delivery rebound to another effect", async () => {
    const store = createMemoryEffectEvidenceStore();
    const ingestion = createEffectEvidenceIngestion({
      authorize: async () => true,
      reconcile: async () => "resolved",
      settle: async () => {},
      store,
    });
    await ingestion.ingest(evidence());
    await expect(ingestion.ingest(evidence("effect-b"))).rejects.toBeInstanceOf(
      EffectEvidenceError,
    );
  });

  test("authorizes tenant and effect binding before persistence", async () => {
    const store = createMemoryEffectEvidenceStore();
    const ingestion = createEffectEvidenceIngestion({
      authorize: async () => false,
      reconcile: async () => "resolved",
      settle: async () => {},
      store,
    });
    await expect(ingestion.ingest(evidence())).rejects.toBeInstanceOf(
      EffectEvidenceError,
    );
    expect(await store.list({ limit: 10 })).toHaveLength(0);
  });

  test("settles confirmed success before reconciliation and retries settlement for duplicate delivery", async () => {
    const events: string[] = [];
    const ingestion = createEffectEvidenceIngestion({
      authorize: async () => true,
      reconcile: async () => {
        events.push("reconcile");
        return "resolved";
      },
      settle: async () => {
        events.push("settle");
      },
      store: createMemoryEffectEvidenceStore(),
    });
    await ingestion.ingest(evidence());
    await ingestion.ingest(evidence());
    expect(events).toEqual(["settle", "reconcile", "settle", "reconcile"]);
  });

  test("does not reconcile confirmed success when settlement fails", async () => {
    let reconciliations = 0;
    const ingestion = createEffectEvidenceIngestion({
      authorize: async () => true,
      reconcile: async () => {
        reconciliations += 1;
        return "resolved";
      },
      settle: async () => {
        throw new Error("ledger unavailable");
      },
      store: createMemoryEffectEvidenceStore(),
    });
    await expect(ingestion.ingest(evidence())).rejects.toThrow(
      "ledger unavailable",
    );
    expect(reconciliations).toBe(0);
  });

  test("ships a normalized evidence schema without raw payload storage", () => {
    const sql = effectEvidencePostgresSchemaSql();
    expect(sql).toContain("effect_evidence");
    expect(sql).toContain("PRIMARY KEY (provider, delivery_id)");
    expect(sql).not.toContain("raw_payload");
  });
});
