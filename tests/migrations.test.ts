import { describe, expect, test } from "bun:test";
import { executionPostgresMigrations } from "../src/migrations";

describe("Execution PostgreSQL migration manifest", () => {
  test("contains every ordered schema upgrade exactly once", () => {
    const migrations = executionPostgresMigrations();
    const ids = migrations.map(({ id }) => id);

    expect(new Set(ids).size).toBe(ids.length);
    expect(ids[0]).toBe("execution@0.2.0");
    expect(ids.indexOf("execution-tenant-inventory@0.3.0")).toBeGreaterThan(
      ids.indexOf("execution@0.2.0"),
    );
    expect(ids.at(-1)).toBe("execution-purpose-bound-installations@0.14.1");
  });

  test("carries the tenant columns required by the current store", () => {
    const tenantInventory = executionPostgresMigrations().find(
      ({ id }) => id === "execution-tenant-inventory@0.3.0",
    );

    expect(tenantInventory?.sql).toContain("tenant_id");
    expect(tenantInventory?.sql).toContain("run_id");
  });
});
