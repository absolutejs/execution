import { describe, expect, test } from "bun:test";
import {
  createEffectRecoveryOperations,
  createMemoryEffectStore,
  EffectRecoveryError,
  type EffectRecord,
} from "../src";

const unknownEffect = (
  effectId: string,
  tenantId = "tenant-a",
): EffectRecord => ({
  actionId: `action:${effectId}`,
  attempts: 1,
  availableAt: 1,
  createdAt: 1,
  effectId,
  error: "Provider outcome is unknown",
  handler: "email.send",
  idempotencyKey: `key:${effectId}`,
  input: { private: "payload" },
  inputDigest: `digest:${effectId}`,
  status: "unknown",
  tenantId,
  updatedAt: 1,
});

const operations = (
  store: ReturnType<typeof createMemoryEffectStore>,
  options: {
    authorize?: () => Promise<boolean>;
    verify?: () => Promise<boolean>;
  } = {},
) =>
  createEffectRecoveryOperations({
    authorize: options.authorize ?? (async () => true),
    id: () => "reconciliation-1",
    now: () => 2,
    store,
    verifyEvidence: options.verify ?? (async () => true),
  });

const request = (effectId: string) => ({
  actorId: "admin-a",
  effectId,
  evidenceReference: "provider:event-1",
  note: "Checked the provider event ledger",
  resolution: "confirmed_succeeded" as const,
  source: "operator" as const,
  tenantId: "tenant-a",
});

describe("effect recovery operations", () => {
  test("creates a default reconciliation id under Bun", async () => {
    const store = createMemoryEffectStore();
    await store.enqueue(unknownEffect("effect-default-id"));
    const recovery = createEffectRecoveryOperations({
      authorize: async () => true,
      now: () => 2,
      store,
      verifyEvidence: async () => true,
    });

    const resolved = await recovery.resolve(request("effect-default-id"));
    expect(resolved.reconciliationHistory[0]?.reconciliationId).toMatch(
      /^[a-f0-9-]{36}$/,
    );
  });

  test("redacts payloads and retains append-only evidence", async () => {
    const store = createMemoryEffectStore();
    await store.enqueue(unknownEffect("effect-1"));
    const recovery = operations(store);
    const [before] = await recovery.inventory({ limit: 10 });
    if (!before) throw new Error("Recovery case was not listed");
    expect("input" in before).toBe(false);
    expect("idempotencyKey" in before).toBe(false);

    const resolved = await recovery.resolve(request("effect-1"));
    expect(resolved.status).toBe("succeeded");
    expect(resolved.reconciliationHistory).toEqual([
      expect.objectContaining({
        actorId: "admin-a",
        evidenceReference: "provider:event-1",
        resolution: "confirmed_succeeded",
      }),
    ]);
  });

  test("fails closed across tenant and evidence boundaries", async () => {
    const store = createMemoryEffectStore();
    await store.enqueue(unknownEffect("effect-2"));
    let authorized = 0;
    const recovery = operations(store, {
      authorize: async () => {
        authorized += 1;
        return true;
      },
      verify: async () => false,
    });
    await expect(
      recovery.resolve({ ...request("effect-2"), tenantId: "tenant-b" }),
    ).rejects.toBeInstanceOf(EffectRecoveryError);
    expect(authorized).toBe(0);
    await expect(recovery.resolve(request("effect-2"))).rejects.toThrow(
      "evidence",
    );
    expect((await store.get("effect-2"))?.status).toBe("unknown");
  });

  test("authorizes before evidence verification", async () => {
    const store = createMemoryEffectStore();
    await store.enqueue(unknownEffect("effect-3"));
    const order: string[] = [];
    const recovery = operations(store, {
      authorize: async () => {
        order.push("authorize");
        return true;
      },
      verify: async () => {
        order.push("evidence");
        return true;
      },
    });
    await recovery.resolve(request("effect-3"));
    expect(order).toEqual(["authorize", "evidence"]);
  });

  test("allows exactly one concurrent resolution", async () => {
    const store = createMemoryEffectStore();
    await store.enqueue(unknownEffect("effect-4"));
    const recovery = operations(store);
    const settled = await Promise.allSettled([
      recovery.resolve(request("effect-4")),
      recovery.resolve({ ...request("effect-4"), actorId: "admin-b" }),
    ]);
    expect(settled.filter(({ status }) => status === "fulfilled")).toHaveLength(
      1,
    );
    expect(await store.listReconciliations("effect-4")).toHaveLength(1);
  });

  test("retries only through an explicit confirmed-not-applied resolution", async () => {
    const store = createMemoryEffectStore();
    await store.enqueue(unknownEffect("effect-5"));
    const original = await store.claimOutbox("dispatcher", 30_000, 1);
    if (!original) throw new Error("Initial effect outbox event was not found");
    await store.publishOutbox(original.eventId, "dispatcher");
    const resolved = await operations(store).resolve({
      ...request("effect-5"),
      resolution: "confirmed_not_applied",
    });
    expect(resolved.status).toBe("pending");
    expect(resolved.reconciliationHistory[0]?.resolution).toBe(
      "confirmed_not_applied",
    );
    const retry = await store.claimOutbox("dispatcher", 30_000, 3);
    expect(retry?.effectId).toBe("effect-5");
    expect(retry?.eventId).toContain(":recovery:");
  });
});
