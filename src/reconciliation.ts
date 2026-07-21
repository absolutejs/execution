import type {
  EffectAdapterInstallationRegistry,
  EffectAdapterInstallationAuthorization,
} from "./adapterInstallations";
import type { EffectAdapterDescriptor } from "./adapterRegistry";
import {
  parseEffectAdapterExecutionEnvelope,
  type EffectAdapterExecutionEnvelope,
  type ResolvedEffectAdapterCredential,
} from "./adapterExecution";
import type { EffectEvidenceOutcome, EffectEvidenceRecord } from "./evidence";
import type { ExecutionSqlClient } from "./postgres";
import type { EffectRecord, EffectStore } from "./types";

export type EffectAdapterHealthSignal = "provider-query" | "provider-webhook";
export type EffectAdapterHealthStatus = "failed" | "healthy" | "warning";

export type EffectAdapterHealthObservation = {
  adapterId: string;
  code: string;
  provider: string;
  scopeId: string;
  signal: EffectAdapterHealthSignal;
  status: EffectAdapterHealthStatus;
  tenantId: string;
};

export type EffectAdapterHealthRecord = EffectAdapterHealthObservation & {
  checkedAt: number;
  failures: number;
  lastFailureAt?: number;
  lastSuccessAt?: number;
  successes: number;
};

export type EffectAdapterHealthStore = {
  list: (input: {
    adapterId?: string;
    limit: number;
    tenantId?: string;
  }) => Promise<EffectAdapterHealthRecord[]>;
  observe: (
    observation: EffectAdapterHealthObservation & { checkedAt: number },
  ) => Promise<EffectAdapterHealthRecord>;
};

export type EffectReconciliationLeaseStore = {
  claim: (input: {
    effectId: string;
    leaseMs: number;
    now: number;
    owner: string;
  }) => Promise<boolean>;
  complete: (input: {
    effectId: string;
    errorCode?: string;
    nextCheckAt: number;
    now: number;
    owner: string;
  }) => Promise<boolean>;
};

export type EffectAdapterReconciliationRunInput = {
  tenantId?: string;
};

export type EffectAdapterQueryResult =
  | { status: "pending" }
  | {
      deliveryId: string;
      eventType: string;
      evidenceReference: string;
      occurredAt: number;
      outcome: EffectEvidenceOutcome;
      providerResourceId?: string;
      status: "resolved";
      verifier: string;
    };

export type EffectAdapterQueryDriver = {
  adapterId: string;
  provider: string;
  query: (
    effect: {
      effectId: string;
      idempotencyKey: string;
      inputDigest: string;
    },
    context: {
      credential: ResolvedEffectAdapterCredential;
      installationId: string;
      signal: AbortSignal;
      tenantId: string;
    },
  ) => Promise<EffectAdapterQueryResult>;
  version: string;
};

export class EffectAdapterReconciliationError extends Error {}

const CODE_PATTERN = /^[a-z][a-z0-9_]{0,63}$/;
const required = (value: string, field: string) => {
  const normalized = value.trim();
  if (!normalized)
    throw new EffectAdapterReconciliationError(`${field} is required`);
  return normalized;
};
const safeCode = (value: string) => {
  if (!CODE_PATTERN.test(value))
    throw new EffectAdapterReconciliationError(
      "Reconciliation health code is invalid",
    );
  return value;
};
const positive = (value: number, field: string) => {
  if (!Number.isSafeInteger(value) || value < 1)
    throw new EffectAdapterReconciliationError(
      `${field} must be a positive safe integer`,
    );
  return value;
};
const healthKey = (value: EffectAdapterHealthObservation) =>
  [value.adapterId, value.tenantId, value.signal, value.scopeId].join("\u0000");

export const createEffectAdapterHealthOperations = (options: {
  now?: () => number;
  store: EffectAdapterHealthStore;
}) => {
  const now = options.now ?? Date.now;
  return {
    list: (input: { adapterId?: string; limit: number; tenantId?: string }) =>
      options.store.list({
        ...input,
        limit: positive(input.limit, "limit"),
      }),
    observe: (input: EffectAdapterHealthObservation) =>
      options.store.observe({
        adapterId: required(input.adapterId, "adapterId"),
        checkedAt: now(),
        code: safeCode(input.code),
        provider: required(input.provider, "provider"),
        scopeId: required(input.scopeId, "scopeId"),
        signal: input.signal,
        status: input.status,
        tenantId: required(input.tenantId, "tenantId"),
      }),
  };
};

export const createMemoryEffectAdapterHealthStore =
  (): EffectAdapterHealthStore => {
    const records = new Map<string, EffectAdapterHealthRecord>();
    return {
      list: async ({ adapterId, limit, tenantId }) =>
        [...records.values()]
          .filter(
            (record) =>
              (!adapterId || record.adapterId === adapterId) &&
              (!tenantId || record.tenantId === tenantId),
          )
          .sort((left, right) => right.checkedAt - left.checkedAt)
          .slice(0, limit)
          .map((record) => structuredClone(record)),
      observe: async (observation) => {
        const existing = records.get(healthKey(observation));
        const success = observation.status === "healthy";
        const record: EffectAdapterHealthRecord = {
          ...observation,
          failures: (existing?.failures ?? 0) + (success ? 0 : 1),
          ...(success
            ? {
                ...(existing?.lastFailureAt
                  ? { lastFailureAt: existing.lastFailureAt }
                  : {}),
                lastSuccessAt: observation.checkedAt,
              }
            : {
                ...(existing?.lastSuccessAt
                  ? { lastSuccessAt: existing.lastSuccessAt }
                  : {}),
                lastFailureAt: observation.checkedAt,
              }),
          successes: (existing?.successes ?? 0) + (success ? 1 : 0),
        };
        records.set(healthKey(observation), structuredClone(record));
        return record;
      },
    };
  };

export const createMemoryEffectReconciliationLeaseStore =
  (): EffectReconciliationLeaseStore => {
    const records = new Map<
      string,
      { leaseExpiresAt?: number; nextCheckAt: number; owner?: string }
    >();
    return {
      claim: async ({ effectId, leaseMs, now, owner }) => {
        const record = records.get(effectId);
        if (
          record &&
          (record.nextCheckAt > now ||
            (record.leaseExpiresAt !== undefined &&
              record.leaseExpiresAt > now))
        )
          return false;
        records.set(effectId, {
          leaseExpiresAt: now + leaseMs,
          nextCheckAt: record?.nextCheckAt ?? 0,
          owner,
        });
        return true;
      },
      complete: async ({ effectId, nextCheckAt, owner }) => {
        const record = records.get(effectId);
        if (!record || record.owner !== owner) return false;
        records.set(effectId, { nextCheckAt });
        return true;
      },
    };
  };

const namespaceOf = (namespace: string) => {
  if (!/^[a-z_][a-z0-9_]*$/.test(namespace))
    throw new EffectAdapterReconciliationError(
      "Reconciliation namespace must be a simple identifier",
    );
  return namespace;
};

export const effectAdapterReconciliationPostgresSchemaSql = (
  namespace = "execution",
) => {
  const ns = namespaceOf(namespace);
  return `CREATE TABLE IF NOT EXISTS ${ns}.adapter_reconciliation_health (
  adapter_id text NOT NULL,
  tenant_id text NOT NULL,
  provider text NOT NULL,
  signal text NOT NULL,
  scope_id text NOT NULL,
  status text NOT NULL,
  code text NOT NULL,
  successes bigint NOT NULL DEFAULT 0,
  failures bigint NOT NULL DEFAULT 0,
  last_success_at bigint,
  last_failure_at bigint,
  checked_at bigint NOT NULL,
  PRIMARY KEY (adapter_id, tenant_id, signal, scope_id)
);
CREATE INDEX IF NOT EXISTS adapter_reconciliation_health_checked_idx ON ${ns}.adapter_reconciliation_health (checked_at DESC);
CREATE TABLE IF NOT EXISTS ${ns}.effect_reconciliation_query_schedule (
  effect_id text PRIMARY KEY REFERENCES ${ns}.effects(effect_id) ON DELETE CASCADE,
  lease_owner text,
  lease_expires_at bigint,
  next_check_at bigint NOT NULL DEFAULT 0,
  attempts integer NOT NULL DEFAULT 0,
  last_error_code text,
  updated_at bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS effect_reconciliation_query_due_idx ON ${ns}.effect_reconciliation_query_schedule (next_check_at, lease_expires_at);`;
};

type HealthRow = {
  adapter_id: string;
  checked_at: number | string;
  code: string;
  failures: number | string;
  last_failure_at: number | string | null;
  last_success_at: number | string | null;
  provider: string;
  scope_id: string;
  signal: EffectAdapterHealthSignal;
  status: EffectAdapterHealthStatus;
  successes: number | string;
  tenant_id: string;
};
const healthFromRow = (row: HealthRow): EffectAdapterHealthRecord => ({
  adapterId: row.adapter_id,
  checkedAt: Number(row.checked_at),
  code: row.code,
  failures: Number(row.failures),
  ...(row.last_failure_at === null
    ? {}
    : { lastFailureAt: Number(row.last_failure_at) }),
  ...(row.last_success_at === null
    ? {}
    : { lastSuccessAt: Number(row.last_success_at) }),
  provider: row.provider,
  scopeId: row.scope_id,
  signal: row.signal,
  status: row.status,
  successes: Number(row.successes),
  tenantId: row.tenant_id,
});

export const createPostgresEffectAdapterHealthStore = (options: {
  client: ExecutionSqlClient;
  namespace?: string;
}): EffectAdapterHealthStore => {
  const ns = namespaceOf(options.namespace ?? "execution");
  const columns =
    "adapter_id, tenant_id, provider, signal, scope_id, status, code, successes, failures, last_success_at, last_failure_at, checked_at";
  return {
    list: async ({ adapterId, limit, tenantId }) => {
      const result = await options.client.query<HealthRow>(
        `SELECT ${columns} FROM ${ns}.adapter_reconciliation_health WHERE ($1::text IS NULL OR tenant_id = $1) AND ($2::text IS NULL OR adapter_id = $2) ORDER BY checked_at DESC LIMIT $3`,
        [tenantId ?? null, adapterId ?? null, limit],
      );
      return result.rows.map(healthFromRow);
    },
    observe: async (observation) => {
      const success = observation.status === "healthy";
      const result = await options.client.query<HealthRow>(
        `INSERT INTO ${ns}.adapter_reconciliation_health
          (adapter_id, tenant_id, provider, signal, scope_id, status, code, successes, failures, last_success_at, last_failure_at, checked_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
         ON CONFLICT (adapter_id, tenant_id, signal, scope_id) DO UPDATE SET
          provider = EXCLUDED.provider,
          status = EXCLUDED.status,
          code = EXCLUDED.code,
          successes = ${ns}.adapter_reconciliation_health.successes + EXCLUDED.successes,
          failures = ${ns}.adapter_reconciliation_health.failures + EXCLUDED.failures,
          last_success_at = COALESCE(EXCLUDED.last_success_at, ${ns}.adapter_reconciliation_health.last_success_at),
          last_failure_at = COALESCE(EXCLUDED.last_failure_at, ${ns}.adapter_reconciliation_health.last_failure_at),
          checked_at = EXCLUDED.checked_at
         RETURNING ${columns}`,
        [
          observation.adapterId,
          observation.tenantId,
          observation.provider,
          observation.signal,
          observation.scopeId,
          observation.status,
          observation.code,
          success ? 1 : 0,
          success ? 0 : 1,
          success ? observation.checkedAt : null,
          success ? null : observation.checkedAt,
          observation.checkedAt,
        ],
      );
      const row = result.rows[0];
      if (!row)
        throw new EffectAdapterReconciliationError(
          "Reconciliation health observation was not retained",
        );
      return healthFromRow(row);
    },
  };
};

export const createPostgresEffectReconciliationLeaseStore = (options: {
  client: ExecutionSqlClient;
  namespace?: string;
}): EffectReconciliationLeaseStore => {
  const ns = namespaceOf(options.namespace ?? "execution");
  return {
    claim: async ({ effectId, leaseMs, now, owner }) => {
      const result = await options.client.query<{ effect_id: string }>(
        `INSERT INTO ${ns}.effect_reconciliation_query_schedule
          (effect_id, lease_owner, lease_expires_at, next_check_at, attempts, updated_at)
         VALUES ($1,$2,$3,0,1,$4)
         ON CONFLICT (effect_id) DO UPDATE SET
          lease_owner = EXCLUDED.lease_owner,
          lease_expires_at = EXCLUDED.lease_expires_at,
          attempts = ${ns}.effect_reconciliation_query_schedule.attempts + 1,
          updated_at = EXCLUDED.updated_at
         WHERE ${ns}.effect_reconciliation_query_schedule.next_check_at <= $4
           AND (${ns}.effect_reconciliation_query_schedule.lease_expires_at IS NULL
             OR ${ns}.effect_reconciliation_query_schedule.lease_expires_at <= $4)
         RETURNING effect_id`,
        [effectId, owner, now + leaseMs, now],
      );
      return result.rows[0] !== undefined;
    },
    complete: async ({ effectId, errorCode, nextCheckAt, now, owner }) => {
      const result = await options.client.query<{ effect_id: string }>(
        `UPDATE ${ns}.effect_reconciliation_query_schedule
         SET lease_owner = NULL, lease_expires_at = NULL, next_check_at = $3,
             last_error_code = $4, updated_at = $5
         WHERE effect_id = $1 AND lease_owner = $2
         RETURNING effect_id`,
        [effectId, owner, nextCheckAt, errorCode ?? null, now],
      );
      return result.rows[0] !== undefined;
    },
  };
};

type QueryDescriptor = Extract<
  EffectAdapterDescriptor["reconciliation"],
  { mode: "query" }
>;

const queryCredential = (
  descriptor: QueryDescriptor,
  credentials: ReadonlyArray<{
    adapterAlias: string;
    destination: string;
    secretAlias: string;
  }>,
) => {
  const matches = credentials.filter(
    ({ adapterAlias }) => adapterAlias === descriptor.query.credentialAlias,
  );
  if (matches.length !== 1)
    throw new EffectAdapterReconciliationError(
      "Reconciliation query credential binding is ambiguous",
    );
  return matches[0]!;
};

const queryDriver = (
  drivers: ReadonlyArray<EffectAdapterQueryDriver>,
  descriptor: EffectAdapterDescriptor,
) => {
  if (descriptor.reconciliation.mode !== "query")
    throw new EffectAdapterReconciliationError(
      "Effect adapter does not support provider-query reconciliation",
    );
  const reconciliation = descriptor.reconciliation;
  const matches = drivers.filter(
    (driver) =>
      driver.adapterId === descriptor.adapterId &&
      driver.provider === reconciliation.query.provider &&
      driver.version === descriptor.version,
  );
  if (matches.length !== 1)
    throw new EffectAdapterReconciliationError(
      "Exact reconciliation query driver is unavailable",
    );
  return matches[0]!;
};

const authorizationInput = (
  effect: EffectRecord,
  envelope: EffectAdapterExecutionEnvelope,
): EffectAdapterInstallationAuthorization => ({
  ...(envelope.destination ? { destination: envelope.destination } : {}),
  effect: envelope.effect,
  installationId: envelope.installationId,
  tenantId: effect.tenantId,
});

export const createEffectAdapterReconciliationRuntime = (options: {
  drivers: ReadonlyArray<EffectAdapterQueryDriver>;
  effects: Pick<EffectStore, "list">;
  health: ReturnType<typeof createEffectAdapterHealthOperations>;
  ingestEvidence: (
    evidence: EffectEvidenceRecord,
    source: "provider_query",
  ) => Promise<unknown>;
  installations: Pick<EffectAdapterInstallationRegistry, "authorize">;
  leaseMs: number;
  leases: EffectReconciliationLeaseStore;
  limit: number;
  now?: () => number;
  resolveCredential: (input: {
    adapterAlias: string;
    destination: string;
    secretAlias: string;
    tenantId: string;
  }) => Promise<string | null | undefined>;
  workerId: string;
}) => {
  const now = options.now ?? Date.now;
  positive(options.leaseMs, "leaseMs");
  positive(options.limit, "limit");
  required(options.workerId, "workerId");

  const reconcile = async (effect: EffectRecord) => {
    const claimedAt = now();
    if (
      !(await options.leases.claim({
        effectId: effect.effectId,
        leaseMs: options.leaseMs,
        now: claimedAt,
        owner: options.workerId,
      }))
    )
      return "skipped" as const;

    let adapter: EffectAdapterDescriptor | undefined;
    let envelope: EffectAdapterExecutionEnvelope | undefined;
    try {
      envelope = parseEffectAdapterExecutionEnvelope(effect.input);
      const authorization = await options.installations.authorize(
        authorizationInput(effect, envelope),
      );
      adapter = authorization.adapter;
      if (adapter.reconciliation.mode !== "query") {
        const completedAt = now();
        await options.leases.complete({
          effectId: effect.effectId,
          nextCheckAt: completedAt + options.leaseMs,
          now: completedAt,
          owner: options.workerId,
        });
        return "skipped" as const;
      }
      const driver = queryDriver(options.drivers, adapter);
      const installed = queryCredential(
        adapter.reconciliation,
        authorization.credentials,
      );
      const value = await options.resolveCredential({
        ...installed,
        tenantId: effect.tenantId,
      });
      if (!value)
        throw new EffectAdapterReconciliationError(
          "Reconciliation query credential is unavailable",
        );
      const declared = adapter.credentialBindings.find(
        ({ alias, destination }) =>
          alias === installed.adapterAlias &&
          destination === installed.destination,
      );
      if (!declared)
        throw new EffectAdapterReconciliationError(
          "Reconciliation query credential is undeclared",
        );
      const result = await driver.query(
        {
          effectId: effect.effectId,
          idempotencyKey: effect.idempotencyKey,
          inputDigest: effect.inputDigest,
        },
        {
          credential: { ...installed, mode: declared.mode, value },
          installationId: envelope.installationId,
          signal: AbortSignal.timeout(options.leaseMs),
          tenantId: effect.tenantId,
        },
      );
      await options.health.observe({
        adapterId: adapter.adapterId,
        code: "query_succeeded",
        provider: adapter.reconciliation.query.provider,
        scopeId: envelope.installationId,
        signal: "provider-query",
        status: "healthy",
        tenantId: effect.tenantId,
      });
      if (result.status === "resolved") {
        await options.ingestEvidence(
          {
            deliveryId: required(result.deliveryId, "deliveryId"),
            effectId: effect.effectId,
            eventType: required(result.eventType, "eventType"),
            evidenceReference: required(
              result.evidenceReference,
              "evidenceReference",
            ),
            occurredAt: result.occurredAt,
            outcome: result.outcome,
            provider: adapter.reconciliation.query.provider,
            ...(result.providerResourceId
              ? { providerResourceId: result.providerResourceId }
              : {}),
            receivedAt: now(),
            tenantId: effect.tenantId,
            verifier: required(result.verifier, "verifier"),
          },
          "provider_query",
        );
      }
      const completedAt = now();
      await options.leases.complete({
        effectId: effect.effectId,
        nextCheckAt:
          completedAt + adapter.reconciliation.query.pollingIntervalMs,
        now: completedAt,
        owner: options.workerId,
      });
      return result.status;
    } catch {
      if (adapter?.reconciliation.mode === "query" && envelope !== undefined)
        await options.health.observe({
          adapterId: adapter.adapterId,
          code: "query_failed",
          provider: adapter.reconciliation.query.provider,
          scopeId: envelope.installationId,
          signal: "provider-query",
          status: "failed",
          tenantId: effect.tenantId,
        });
      const completedAt = now();
      await options.leases.complete({
        effectId: effect.effectId,
        errorCode: "query_failed",
        nextCheckAt:
          completedAt +
          (adapter?.reconciliation.mode === "query"
            ? adapter.reconciliation.query.pollingIntervalMs
            : options.leaseMs),
        now: completedAt,
        owner: options.workerId,
      });
      return "failed" as const;
    }
  };

  return {
    runOnce: async (input: EffectAdapterReconciliationRunInput = {}) => {
      const tenantId =
        input.tenantId === undefined
          ? undefined
          : required(input.tenantId, "tenantId");
      const effects = await options.effects.list({
        limit: options.limit,
        status: "unknown",
        ...(tenantId === undefined ? {} : { tenantId }),
      });
      const outcomes = [];
      for (const effect of effects) outcomes.push(await reconcile(effect));
      return {
        failed: outcomes.filter((outcome) => outcome === "failed").length,
        pending: outcomes.filter((outcome) => outcome === "pending").length,
        resolved: outcomes.filter((outcome) => outcome === "resolved").length,
        scanned: effects.length,
        skipped: outcomes.filter((outcome) => outcome === "skipped").length,
      };
    },
  };
};
