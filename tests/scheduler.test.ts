import { describe, expect, test } from "bun:test";
import {
  createManagedEffectReconciliationScheduler,
  createMemoryEffectReconciliationSchedulerStore,
  effectReconciliationSchedulerPostgresSchemaSql,
} from "../src";

const result = {
  failed: 0,
  pending: 1,
  resolved: 2,
  scanned: 4,
  skipped: 1,
};

const setup = (options?: { fail?: boolean }) => {
  let current = 1_000;
  let runs = 0;
  const errors: unknown[] = [];
  const store = createMemoryEffectReconciliationSchedulerStore();
  const scheduler = createManagedEffectReconciliationScheduler({
    defaultPolicy: { enabled: false, intervalMs: 5_000 },
    leaseMs: 2_000,
    now: () => current,
    onError: (error) => errors.push(error),
    pollMs: 1_000,
    run: async () => {
      runs += 1;
      if (options?.fail) throw new Error("private provider response");
      return result;
    },
    schedulerId: "provider-reconciliation",
    store,
    workerId: "worker-a",
  });
  return {
    advance: (milliseconds: number) => {
      current += milliseconds;
    },
    errors,
    runs: () => runs,
    scheduler,
    store,
  };
};

describe("managed effect reconciliation scheduler", () => {
  test("initializes disabled and does no work until explicitly enabled", async () => {
    const harness = setup();
    await harness.scheduler.initialize();
    expect(await harness.scheduler.runDueOnce()).toEqual({ ran: false });
    expect(harness.runs()).toBe(0);

    await harness.scheduler.configure({ enabled: true, intervalMs: 5_000 });
    expect(await harness.scheduler.runDueOnce()).toEqual({ ran: true, result });
    expect(harness.runs()).toBe(1);
    expect((await harness.scheduler.posture()).policy).toMatchObject({
      enabled: true,
      lastResult: result,
      nextRunAt: 6_000,
    });
  });

  test("retains policy across initialization and runs only when due", async () => {
    const harness = setup();
    await harness.scheduler.configure({ enabled: true, intervalMs: 10_000 });
    await harness.scheduler.initialize();
    expect((await harness.scheduler.posture()).policy.intervalMs).toBe(10_000);
    await harness.scheduler.runDueOnce();
    harness.advance(9_999);
    expect(await harness.scheduler.runDueOnce()).toEqual({ ran: false });
    harness.advance(1);
    expect((await harness.scheduler.runDueOnce()).ran).toBe(true);
    expect(harness.runs()).toBe(2);
  });

  test("a shared durable claim prevents replica double-runs", async () => {
    const harness = setup();
    const replica = createManagedEffectReconciliationScheduler({
      defaultPolicy: { enabled: false, intervalMs: 5_000 },
      leaseMs: 2_000,
      now: () => 1_000,
      pollMs: 1_000,
      run: async () => {
        throw new Error("replica must not run");
      },
      schedulerId: "provider-reconciliation",
      store: harness.store,
      workerId: "worker-b",
    });
    await harness.scheduler.configure({ enabled: true, intervalMs: 5_000 });
    const runs = await Promise.all([
      harness.scheduler.runDueOnce(),
      replica.runDueOnce(),
    ]);
    expect(runs.filter(({ ran }) => ran)).toHaveLength(1);
    expect(harness.runs()).toBe(1);
  });

  test("retains only a bounded failure code", async () => {
    const harness = setup({ fail: true });
    await harness.scheduler.configure({ enabled: true, intervalMs: 5_000 });
    expect(await harness.scheduler.runDueOnce()).toEqual({
      errorCode: "run_failed",
      ran: true,
    });
    const posture = await harness.scheduler.posture();
    expect(posture.policy.lastErrorCode).toBe("run_failed");
    expect(JSON.stringify(posture)).not.toContain("private provider response");
    expect(harness.errors).toHaveLength(1);
  });

  test("ships a replica-safe durable schedule schema", () => {
    const sql = effectReconciliationSchedulerPostgresSchemaSql();
    expect(sql).toContain("effect_reconciliation_scheduler");
    expect(sql).toContain("enabled boolean NOT NULL DEFAULT false");
    expect(sql).toContain("lease_expires_at");
    expect(sql).not.toContain("raw_error");
  });
});
