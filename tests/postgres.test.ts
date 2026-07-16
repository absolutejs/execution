import { describe, expect, test } from "bun:test";
import {
  createPostgresEffectStore,
  executionPostgresSchemaSql,
  type EffectRecord,
  type ExecutionSqlClient,
} from "../src";

describe("PostgreSQL effect store", () => {
  test("ships effects, attempt history, and an outbox schema", () => {
    const sql = executionPostgresSchemaSql("agent_execution");
    expect(sql).toContain("effect_attempts");
    expect(sql).toContain("effect_outbox");
    expect(() => executionPostgresSchemaSql("bad-name")).toThrow();
  });

  test("creates the effect and outbox event in one SQL statement", async () => {
    const calls: string[] = [];
    const client: ExecutionSqlClient = {
      query: async <Row>(text: string) => {
        calls.push(text);
        return { rows: [{ effect_id: "effect-1" } as Row] };
      },
    };
    const store = createPostgresEffectStore({ client });
    const effect: EffectRecord = {
      actionId: "action-1",
      attempts: 0,
      availableAt: 0,
      createdAt: 0,
      effectId: "effect-1",
      handler: "send",
      idempotencyKey: "key-1",
      input: {},
      inputDigest: "digest",
      status: "pending",
      updatedAt: 0,
    };
    expect(await store.enqueue(effect)).toBe(true);
    expect(calls).toHaveLength(1);
    expect(calls[0]).toContain("WITH inserted AS");
    expect(calls[0]).toContain("effect_outbox");
  });

  test("claims with row locks and skip-locked semantics", async () => {
    const calls: string[] = [];
    const client: ExecutionSqlClient = {
      query: async <Row>(text: string) => {
        calls.push(text);
        return { rows: [] as Row[] };
      },
    };
    const store = createPostgresEffectStore({ client });
    await store.claim("worker", 30_000, 1);
    await store.claimOutbox("worker", 30_000, 1);
    expect(calls.every((sql) => sql.includes("SKIP LOCKED"))).toBe(true);
  });
});
