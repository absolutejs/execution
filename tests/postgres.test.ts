import { describe, expect, test } from "bun:test";
import { SQL } from "bun";
import {
  createPostgresEffectStore,
  effectRecoveryPostgresSchemaSql,
  executionPostgresSchemaSql,
  executionTenantInventoryPostgresSchemaSql,
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

  test("ships tenant inventory and tenant-scoped idempotency migration", () => {
    const sql = executionTenantInventoryPostgresSchemaSql();
    expect(sql).toContain("tenant_id");
    expect(sql).toContain("effects_tenant_idempotency_idx");
    expect(sql).toContain("DROP CONSTRAINT IF EXISTS");
  });

  test("ships append-only effect reconciliation history", () => {
    const sql = effectRecoveryPostgresSchemaSql();
    expect(sql).toContain("effect_reconciliations");
    expect(sql).toContain("evidence_reference");
    expect(() => effectRecoveryPostgresSchemaSql("bad-name")).toThrow();
  });

  test("resolves an unknown effect and records evidence atomically", async () => {
    const calls: Array<{ text: string; values?: readonly unknown[] }> = [];
    const store = createPostgresEffectStore({
      client: {
        query: async <Row>(text: string, values?: readonly unknown[]) => {
          calls.push({ text, values });
          return { rows: [{ effect_id: "effect-1" } as Row] };
        },
      },
    });

    expect(
      await store.resolveUnknown({
        effectId: "effect-1",
        reconciliation: {
          actorId: "admin-1",
          createdAt: 2,
          effectId: "effect-1",
          evidenceReference: "provider:event-1",
          note: "Verified in the provider ledger",
          reconciliationId: "reconciliation-1",
          resolution: "confirmed_succeeded",
          source: "operator",
          tenantId: "tenant-1",
        },
        result: { evidenceReference: "provider:event-1" },
        status: "succeeded",
        updatedAt: 2,
      }),
    ).toBe(true);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.text).toContain("WITH updated AS");
    expect(calls[0]?.text).toContain("effect_reconciliations");
    expect(calls[0]?.text).toContain("effect_outbox");
    expect(calls[0]?.text).toContain("tenant_id = $5");
  });

  test("quarantines only the exact attempt with a bounded provider reference", async () => {
    const calls: Array<{ text: string; values?: readonly unknown[] }> = [];
    const store = createPostgresEffectStore({
      client: {
        query: async <Row>(text: string, values?: readonly unknown[]) => {
          calls.push({ text, values });
          return { rows: [{ effect_id: "effect-1" } as Row] };
        },
      },
    });
    await store.quarantineUnknown(
      "effect-1",
      2,
      {
        error: "completion lease lost",
        reconciliationReference: {
          adapterId: "provider.adapter",
          provider: "provider",
          resourceId: "resource-1",
        },
      },
      3,
    );

    expect(calls[0]?.text).toContain("attempts = $2");
    expect(calls[0]?.text).toContain("status = 'leased'");
    expect(calls[0]?.text).not.toContain("lease_owner = $2");
    expect(String(calls[0]?.values?.[2])).toContain("resource-1");
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
      tenantId: "tenant-1",
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

  test("casts lease parameters for Bun's native PostgreSQL client", async () => {
    const queries: string[] = [];
    const store = createPostgresEffectStore({
      client: {
        query: async <Row>(text: string) => {
          queries.push(text);
          return { rows: [] as Row[] };
        },
      },
    });

    await store.claim("worker", 30_000, 1);
    await store.claimEffect("effect", "worker", 30_000, 1);
    await store.heartbeat("effect", "worker", 30_000, 1);

    expect(queries.join("\n")).toContain("$2::text");
    expect(queries.join("\n")).toContain("$3::bigint");
  });

  test("parses serialized JSON parameters as text before converting to jsonb", async () => {
    const queries: string[] = [];
    const store = createPostgresEffectStore({
      client: {
        query: async <Row>(text: string) => {
          queries.push(text);
          return { rows: [{ effect_id: "effect-1" } as Row] };
        },
      },
    });
    const effect: EffectRecord = {
      actionId: "action-1",
      attempts: 0,
      availableAt: 0,
      createdAt: 0,
      effectId: "effect-1",
      handler: "send",
      idempotencyKey: "key-1",
      input: { canary: "json-object" },
      inputDigest: "digest",
      status: "pending",
      tenantId: "tenant-1",
      updatedAt: 0,
    };

    await store.enqueue(effect);
    await store.claimEffect("effect-1", "worker-1", 30_000, 1);
    await store.succeed("effect-1", "worker-1", { ok: true }, 2);

    expect(queries.join("\n")).not.toMatch(/\$\d+::jsonb/u);
    expect(queries.join("\n")).toContain("$9::text::jsonb");
    expect(queries.join("\n")).toContain("$4::text::jsonb");
  });

  const databaseUrl = process.env.EXECUTION_TEST_DATABASE_URL;
  const databaseTest = databaseUrl === undefined ? test.skip : test;

  databaseTest(
    "preserves effect records through Bun SQL jsonb bindings",
    async () => {
      const namespace = `execution_jsonb_${crypto
        .randomUUID()
        .replaceAll("-", "_")}`;
      const sql = new SQL({
        max: 1,
        prepare: false,
        url: databaseUrl!,
      });
      const client: ExecutionSqlClient = {
        query: async <Row>(text: string, values: readonly unknown[] = []) => ({
          rows: Array.from((await sql.unsafe(text, [...values])) as Row[]),
        }),
      };
      const store = createPostgresEffectStore({ client, namespace });
      const effect: EffectRecord = {
        actionId: "action-bun",
        attempts: 0,
        availableAt: 1,
        createdAt: 1,
        effectId: "effect-bun",
        handler: "send",
        idempotencyKey: "key-bun",
        input: { canary: "json-object" },
        inputDigest: "digest-bun",
        status: "pending",
        tenantId: "tenant-bun",
        updatedAt: 1,
      };

      try {
        await sql.unsafe(executionPostgresSchemaSql(namespace));
        await sql.unsafe(executionTenantInventoryPostgresSchemaSql(namespace));
        expect(await store.enqueue(effect)).toBe(true);

        const claimed = await store.claimEffect(
          effect.effectId,
          "worker-bun",
          30_000,
          2,
        );
        expect(claimed).toMatchObject({
          actionId: effect.actionId,
          effectId: effect.effectId,
          input: effect.input,
          status: "leased",
        });

        expect(
          await store.succeed(
            effect.effectId,
            "worker-bun",
            { canary: "result-object" },
            3,
          ),
        ).toBe(true);
        expect(await store.get(effect.effectId)).toMatchObject({
          actionId: effect.actionId,
          effectId: effect.effectId,
          input: effect.input,
          result: { canary: "result-object" },
          status: "succeeded",
        });
      } finally {
        await sql.unsafe(`DROP SCHEMA IF EXISTS ${namespace} CASCADE`);
        await sql.close({ timeout: 5 });
      }
    },
  );
});
