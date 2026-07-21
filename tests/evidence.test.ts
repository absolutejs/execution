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
      reconcile: async () => {
        reconciliations += 1;
        return reconciliations === 1 ? "resolved" : "already_terminal";
      },
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
      reconcile: async () => "resolved",
      store,
    });
    await ingestion.ingest(evidence());
    await expect(ingestion.ingest(evidence("effect-b"))).rejects.toBeInstanceOf(
      EffectEvidenceError,
    );
  });

  test("ships a normalized evidence schema without raw payload storage", () => {
    const sql = effectEvidencePostgresSchemaSql();
    expect(sql).toContain("effect_evidence");
    expect(sql).toContain("PRIMARY KEY (provider, delivery_id)");
    expect(sql).not.toContain("raw_payload");
  });
});
