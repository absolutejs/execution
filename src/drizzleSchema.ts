import { sql } from "drizzle-orm";
import {
  bigint,
  boolean,
  customType,
  index,
  integer,
  pgSchema,
  primaryKey,
  text,
  uniqueIndex,
} from "drizzle-orm/pg-core";
import type {
  EffectAdapterCertification,
  EffectAdapterDescriptor,
} from "./adapterRegistry";
import type { EffectAdapterInstallationPolicy } from "./adapterInstallations";
import type { EffectRecord, EffectStatus } from "./types";

const portableJsonb = customType<{ data: unknown; driverData: unknown }>({
  dataType: () => "jsonb",
  fromDriver: (value) =>
    typeof value === "string" ? JSON.parse(value) : value,
  toDriver: (value) => JSON.stringify(value),
});
export const encodedJsonb = <Value>(value: Value) =>
  sql<Value>`${JSON.stringify(value)}::text::jsonb`;
const namespaceOf = (value: string) => {
  if (!/^[a-z_][a-z0-9_]*$/.test(value))
    throw new Error("Execution namespace must be a simple identifier");
  return value;
};

export const executionDrizzleSchema = (namespace = "execution") => {
  const schema = pgSchema(namespaceOf(namespace));
  const effects = schema.table(
    "effects",
    {
      action_id: text().notNull(),
      attempts: integer().notNull().default(0),
      available_at: bigint({ mode: "number" }).notNull(),
      created_at: bigint({ mode: "number" }).notNull(),
      data: portableJsonb().$type<EffectRecord>().notNull(),
      effect_id: text().primaryKey(),
      handler: text().notNull(),
      idempotency_key: text().notNull(),
      input_digest: text().notNull(),
      lease_expires_at: bigint({ mode: "number" }),
      lease_owner: text(),
      run_id: text(),
      status: text().$type<EffectStatus>().notNull(),
      tenant_id: text().notNull(),
      updated_at: bigint({ mode: "number" }).notNull(),
    },
    (table) => [
      uniqueIndex("effects_tenant_idempotency_idx").on(
        table.tenant_id,
        table.idempotency_key,
      ),
      index("effects_claim_idx").on(
        table.status,
        table.available_at,
        table.lease_expires_at,
      ),
      index("effects_tenant_inventory_idx").on(
        table.tenant_id,
        table.created_at.desc(),
      ),
      index("effects_run_inventory_idx").on(
        table.run_id,
        table.created_at.desc(),
      ),
    ],
  );
  const attempts = schema.table(
    "effect_attempts",
    {
      attempt_id: text().primaryKey(),
      effect_id: text()
        .notNull()
        .references(() => effects.effect_id, { onDelete: "cascade" }),
      error: text(),
      finished_at: bigint({ mode: "number" }),
      kind: text().notNull(),
      number: integer().notNull(),
      outcome: text().notNull(),
      started_at: bigint({ mode: "number" }).notNull(),
      worker_id: text().notNull(),
    },
    (table) => [
      index("effect_attempts_effect_idx").on(table.effect_id, table.started_at),
    ],
  );
  const outbox = schema.table(
    "effect_outbox",
    {
      attempts: integer().notNull().default(0),
      created_at: bigint({ mode: "number" }).notNull(),
      effect_id: text()
        .notNull()
        .references(() => effects.effect_id, { onDelete: "cascade" }),
      event_id: text().primaryKey(),
      lease_expires_at: bigint({ mode: "number" }),
      lease_owner: text(),
    },
    (table) => [
      index("effect_outbox_claim_idx").on(
        table.lease_expires_at,
        table.created_at,
      ),
    ],
  );
  const reconciliations = schema.table(
    "effect_reconciliations",
    {
      actor_id: text().notNull(),
      created_at: bigint({ mode: "number" }).notNull(),
      effect_id: text()
        .notNull()
        .references(() => effects.effect_id, { onDelete: "cascade" }),
      evidence_reference: text().notNull(),
      note: text().notNull(),
      reconciliation_id: text().primaryKey(),
      resolution: text().notNull(),
      source: text().notNull(),
      tenant_id: text().notNull(),
    },
    (table) => [
      index("effect_reconciliations_effect_idx").on(
        table.effect_id,
        table.created_at,
      ),
    ],
  );
  const evidence = schema.table(
    "effect_evidence",
    {
      delivery_id: text().notNull(),
      effect_id: text()
        .notNull()
        .references(() => effects.effect_id, { onDelete: "cascade" }),
      event_type: text().notNull(),
      evidence_reference: text().notNull(),
      occurred_at: bigint({ mode: "number" }).notNull(),
      outcome: text().notNull(),
      provider: text().notNull(),
      provider_resource_id: text(),
      received_at: bigint({ mode: "number" }).notNull(),
      tenant_id: text().notNull(),
      verifier: text().notNull(),
    },
    (table) => [
      primaryKey({ columns: [table.provider, table.delivery_id] }),
      index("effect_evidence_effect_idx").on(
        table.effect_id,
        table.received_at.desc(),
      ),
    ],
  );
  const adapterRegistry = schema.table(
    "adapter_registry",
    {
      active: boolean().notNull().default(false),
      adapter_id: text().primaryKey(),
      certification: portableJsonb().$type<EffectAdapterCertification>(),
      descriptor: portableJsonb().$type<EffectAdapterDescriptor>().notNull(),
      descriptor_digest: text().notNull(),
      registered_at: bigint({ mode: "number" }).notNull(),
      updated_at: bigint({ mode: "number" }).notNull(),
      version: text().notNull(),
    },
    (table) => [
      index("adapter_registry_active_idx").on(table.active, table.adapter_id),
    ],
  );
  const installations = schema.table(
    "adapter_installations",
    {
      adapter_id: text().notNull(),
      adapter_version: text().notNull(),
      descriptor_digest: text().notNull(),
      enabled: boolean().notNull().default(false),
      installation_id: text().primaryKey(),
      installed_at: bigint({ mode: "number" }).notNull(),
      policy: portableJsonb()
        .$type<EffectAdapterInstallationPolicy>()
        .notNull(),
      tenant_id: text().notNull(),
      updated_at: bigint({ mode: "number" }).notNull(),
    },
    (table) => [
      index("adapter_installations_tenant_idx").on(
        table.tenant_id,
        table.installation_id,
      ),
      index("adapter_installations_enabled_idx").on(
        table.enabled,
        table.adapter_id,
      ),
    ],
  );
  const health = schema.table(
    "adapter_reconciliation_health",
    {
      adapter_id: text().notNull(),
      checked_at: bigint({ mode: "number" }).notNull(),
      code: text().notNull(),
      failures: bigint({ mode: "number" }).notNull().default(0),
      last_failure_at: bigint({ mode: "number" }),
      last_success_at: bigint({ mode: "number" }),
      provider: text().notNull(),
      scope_id: text().notNull(),
      signal: text().notNull(),
      status: text().notNull(),
      successes: bigint({ mode: "number" }).notNull().default(0),
      tenant_id: text().notNull(),
    },
    (table) => [
      primaryKey({
        columns: [
          table.adapter_id,
          table.tenant_id,
          table.signal,
          table.scope_id,
        ],
      }),
      index("adapter_reconciliation_health_checked_idx").on(
        table.checked_at.desc(),
      ),
    ],
  );
  const querySchedule = schema.table(
    "effect_reconciliation_query_schedule",
    {
      attempts: integer().notNull().default(0),
      effect_id: text()
        .primaryKey()
        .references(() => effects.effect_id, { onDelete: "cascade" }),
      last_error_code: text(),
      lease_expires_at: bigint({ mode: "number" }),
      lease_owner: text(),
      next_check_at: bigint({ mode: "number" }).notNull().default(0),
      updated_at: bigint({ mode: "number" }).notNull(),
    },
    (table) => [
      index("effect_reconciliation_query_due_idx").on(
        table.next_check_at,
        table.lease_expires_at,
      ),
    ],
  );
  const scheduler = schema.table(
    "effect_reconciliation_scheduler",
    {
      enabled: boolean().notNull().default(false),
      interval_ms: bigint({ mode: "number" }).notNull(),
      last_completed_at: bigint({ mode: "number" }),
      last_error_code: text(),
      last_failed: integer(),
      last_pending: integer(),
      last_resolved: integer(),
      last_scanned: integer(),
      last_skipped: integer(),
      last_started_at: bigint({ mode: "number" }),
      lease_expires_at: bigint({ mode: "number" }),
      lease_owner: text(),
      next_run_at: bigint({ mode: "number" }).notNull(),
      scheduler_id: text().primaryKey(),
      updated_at: bigint({ mode: "number" }).notNull(),
    },
    (table) => [
      index("effect_reconciliation_scheduler_due_idx").on(
        table.enabled,
        table.next_run_at,
        table.lease_expires_at,
      ),
    ],
  );

  return {
    adapterRegistry,
    attempts,
    effects,
    evidence,
    health,
    installations,
    outbox,
    querySchedule,
    reconciliations,
    scheduler,
  };
};
