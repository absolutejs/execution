import type { ExecutionSqlClient } from "./postgres";

export type EffectReconciliationSchedulerResult = {
  failed: number;
  pending: number;
  resolved: number;
  scanned: number;
  skipped: number;
};

export type EffectReconciliationSchedulerPolicy = {
  enabled: boolean;
  intervalMs: number;
};

export type EffectReconciliationSchedulerRecord =
  EffectReconciliationSchedulerPolicy & {
    lastCompletedAt?: number;
    lastErrorCode?: string;
    lastResult?: EffectReconciliationSchedulerResult;
    lastStartedAt?: number;
    nextRunAt: number;
    schedulerId: string;
    updatedAt: number;
  };

export type EffectReconciliationSchedulerStore = {
  claim: (input: {
    leaseMs: number;
    now: number;
    owner: string;
    schedulerId: string;
  }) => Promise<boolean>;
  complete: (input: {
    errorCode?: string;
    now: number;
    owner: string;
    result?: EffectReconciliationSchedulerResult;
    schedulerId: string;
  }) => Promise<boolean>;
  configure: (
    input: EffectReconciliationSchedulerPolicy & {
      now: number;
      schedulerId: string;
    },
  ) => Promise<EffectReconciliationSchedulerRecord>;
  initialize: (
    input: EffectReconciliationSchedulerPolicy & {
      now: number;
      schedulerId: string;
    },
  ) => Promise<EffectReconciliationSchedulerRecord>;
  read: (
    schedulerId: string,
  ) => Promise<EffectReconciliationSchedulerRecord | undefined>;
};

export class EffectReconciliationSchedulerError extends Error {}

const positive = (value: number, field: string) => {
  if (!Number.isSafeInteger(value) || value < 1)
    throw new EffectReconciliationSchedulerError(
      `${field} must be a positive safe integer`,
    );
  return value;
};

const required = (value: string, field: string) => {
  const normalized = value.trim();
  if (!normalized)
    throw new EffectReconciliationSchedulerError(`${field} is required`);
  return normalized;
};

const validateResult = (
  result: EffectReconciliationSchedulerResult,
): EffectReconciliationSchedulerResult => ({
  failed: positiveOrZero(result.failed, "result.failed"),
  pending: positiveOrZero(result.pending, "result.pending"),
  resolved: positiveOrZero(result.resolved, "result.resolved"),
  scanned: positiveOrZero(result.scanned, "result.scanned"),
  skipped: positiveOrZero(result.skipped, "result.skipped"),
});

const positiveOrZero = (value: number, field: string) => {
  if (!Number.isSafeInteger(value) || value < 0)
    throw new EffectReconciliationSchedulerError(
      `${field} must be a non-negative safe integer`,
    );
  return value;
};

type MemoryRecord = EffectReconciliationSchedulerRecord & {
  leaseExpiresAt?: number;
  leaseOwner?: string;
};

export const createMemoryEffectReconciliationSchedulerStore =
  (): EffectReconciliationSchedulerStore => {
    const records = new Map<string, MemoryRecord>();
    const publicRecord = ({
      leaseExpiresAt,
      leaseOwner,
      ...record
    }: MemoryRecord) => {
      void leaseExpiresAt;
      void leaseOwner;
      return structuredClone(record);
    };
    return {
      claim: async ({ leaseMs, now, owner, schedulerId }) => {
        const record = records.get(schedulerId);
        if (
          !record?.enabled ||
          record.nextRunAt > now ||
          (record.leaseExpiresAt !== undefined && record.leaseExpiresAt > now)
        )
          return false;
        records.set(schedulerId, {
          ...record,
          lastStartedAt: now,
          leaseExpiresAt: now + leaseMs,
          leaseOwner: owner,
          updatedAt: now,
        });
        return true;
      },
      complete: async ({ errorCode, now, owner, result, schedulerId }) => {
        const record = records.get(schedulerId);
        if (!record || record.leaseOwner !== owner) return false;
        records.set(schedulerId, {
          ...record,
          lastCompletedAt: now,
          ...(errorCode
            ? { lastErrorCode: required(errorCode, "errorCode") }
            : {}),
          ...(result ? { lastResult: validateResult(result) } : {}),
          leaseExpiresAt: undefined,
          leaseOwner: undefined,
          nextRunAt: now + record.intervalMs,
          updatedAt: now,
        });
        if (!errorCode) delete records.get(schedulerId)?.lastErrorCode;
        return true;
      },
      configure: async ({ enabled, intervalMs, now, schedulerId }) => {
        const id = required(schedulerId, "schedulerId");
        const existing = records.get(id);
        const record: MemoryRecord = {
          ...(existing ?? {
            nextRunAt: now,
            schedulerId: id,
          }),
          enabled,
          intervalMs: positive(intervalMs, "intervalMs"),
          nextRunAt:
            enabled && !existing?.enabled ? now : (existing?.nextRunAt ?? now),
          updatedAt: now,
        };
        records.set(id, record);
        return publicRecord(record);
      },
      initialize: async ({ enabled, intervalMs, now, schedulerId }) => {
        const id = required(schedulerId, "schedulerId");
        const existing = records.get(id);
        if (existing) return publicRecord(existing);
        const record: MemoryRecord = {
          enabled,
          intervalMs: positive(intervalMs, "intervalMs"),
          nextRunAt: now,
          schedulerId: id,
          updatedAt: now,
        };
        records.set(id, record);
        return publicRecord(record);
      },
      read: async (schedulerId) => {
        const record = records.get(required(schedulerId, "schedulerId"));
        return record ? publicRecord(record) : undefined;
      },
    };
  };

const namespaceOf = (namespace: string) => {
  if (!/^[a-z_][a-z0-9_]*$/.test(namespace))
    throw new EffectReconciliationSchedulerError(
      "Scheduler namespace must be a simple identifier",
    );
  return namespace;
};

export const effectReconciliationSchedulerPostgresSchemaSql = (
  namespace = "execution",
) => {
  const ns = namespaceOf(namespace);
  return `CREATE TABLE IF NOT EXISTS ${ns}.effect_reconciliation_scheduler (
  scheduler_id text PRIMARY KEY,
  enabled boolean NOT NULL DEFAULT false,
  interval_ms bigint NOT NULL,
  next_run_at bigint NOT NULL,
  lease_owner text,
  lease_expires_at bigint,
  last_started_at bigint,
  last_completed_at bigint,
  last_error_code text,
  last_scanned integer,
  last_pending integer,
  last_resolved integer,
  last_failed integer,
  last_skipped integer,
  updated_at bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS effect_reconciliation_scheduler_due_idx ON ${ns}.effect_reconciliation_scheduler (enabled, next_run_at, lease_expires_at);`;
};

type SchedulerRow = {
  enabled: boolean;
  interval_ms: number | string;
  last_completed_at: number | string | null;
  last_error_code: string | null;
  last_failed: number | null;
  last_pending: number | null;
  last_resolved: number | null;
  last_scanned: number | null;
  last_skipped: number | null;
  last_started_at: number | string | null;
  next_run_at: number | string;
  scheduler_id: string;
  updated_at: number | string;
};

const schedulerColumns =
  "scheduler_id, enabled, interval_ms, next_run_at, last_started_at, last_completed_at, last_error_code, last_scanned, last_pending, last_resolved, last_failed, last_skipped, updated_at";

const fromRow = (row: SchedulerRow): EffectReconciliationSchedulerRecord => ({
  enabled: row.enabled,
  intervalMs: Number(row.interval_ms),
  ...(row.last_completed_at === null
    ? {}
    : { lastCompletedAt: Number(row.last_completed_at) }),
  ...(row.last_error_code === null
    ? {}
    : { lastErrorCode: row.last_error_code }),
  ...(row.last_scanned === null ||
  row.last_pending === null ||
  row.last_resolved === null ||
  row.last_failed === null ||
  row.last_skipped === null
    ? {}
    : {
        lastResult: {
          failed: row.last_failed,
          pending: row.last_pending,
          resolved: row.last_resolved,
          scanned: row.last_scanned,
          skipped: row.last_skipped,
        },
      }),
  ...(row.last_started_at === null
    ? {}
    : { lastStartedAt: Number(row.last_started_at) }),
  nextRunAt: Number(row.next_run_at),
  schedulerId: row.scheduler_id,
  updatedAt: Number(row.updated_at),
});

export const createPostgresEffectReconciliationSchedulerStore = (options: {
  client: ExecutionSqlClient;
  namespace?: string;
}): EffectReconciliationSchedulerStore => {
  const ns = namespaceOf(options.namespace ?? "execution");
  const one = async (text: string, values: readonly unknown[]) => {
    const result = await options.client.query<SchedulerRow>(text, values);
    const row = result.rows[0];
    if (!row)
      throw new EffectReconciliationSchedulerError(
        "Reconciliation scheduler record is unavailable",
      );
    return fromRow(row);
  };
  return {
    claim: async ({ leaseMs, now, owner, schedulerId }) => {
      const result = await options.client.query<{ scheduler_id: string }>(
        `UPDATE ${ns}.effect_reconciliation_scheduler
         SET lease_owner = $2, lease_expires_at = $3, last_started_at = $4, updated_at = $4
         WHERE scheduler_id = $1 AND enabled = true AND next_run_at <= $4
           AND (lease_expires_at IS NULL OR lease_expires_at <= $4)
         RETURNING scheduler_id`,
        [schedulerId, owner, now + leaseMs, now],
      );
      return result.rows[0] !== undefined;
    },
    complete: async ({ errorCode, now, owner, result, schedulerId }) => {
      const validated = result ? validateResult(result) : undefined;
      const completed = await options.client.query<{ scheduler_id: string }>(
        `UPDATE ${ns}.effect_reconciliation_scheduler
         SET lease_owner = NULL, lease_expires_at = NULL,
             next_run_at = $3 + interval_ms, last_completed_at = $3,
             last_error_code = $4, last_scanned = $5, last_pending = $6,
             last_resolved = $7, last_failed = $8, last_skipped = $9,
             updated_at = $3
         WHERE scheduler_id = $1 AND lease_owner = $2
         RETURNING scheduler_id`,
        [
          schedulerId,
          owner,
          now,
          errorCode ? required(errorCode, "errorCode") : null,
          validated?.scanned ?? null,
          validated?.pending ?? null,
          validated?.resolved ?? null,
          validated?.failed ?? null,
          validated?.skipped ?? null,
        ],
      );
      return completed.rows[0] !== undefined;
    },
    configure: ({ enabled, intervalMs, now, schedulerId }) =>
      one(
        `INSERT INTO ${ns}.effect_reconciliation_scheduler
          (scheduler_id, enabled, interval_ms, next_run_at, updated_at)
         VALUES ($1,$2,$3,$4,$4)
         ON CONFLICT (scheduler_id) DO UPDATE SET
          enabled = EXCLUDED.enabled, interval_ms = EXCLUDED.interval_ms,
          next_run_at = CASE
            WHEN EXCLUDED.enabled AND NOT ${ns}.effect_reconciliation_scheduler.enabled THEN EXCLUDED.next_run_at
            ELSE ${ns}.effect_reconciliation_scheduler.next_run_at
          END,
          updated_at = EXCLUDED.updated_at
         RETURNING ${schedulerColumns}`,
        [
          required(schedulerId, "schedulerId"),
          enabled,
          positive(intervalMs, "intervalMs"),
          now,
        ],
      ),
    initialize: ({ enabled, intervalMs, now, schedulerId }) =>
      one(
        `INSERT INTO ${ns}.effect_reconciliation_scheduler
          (scheduler_id, enabled, interval_ms, next_run_at, updated_at)
         VALUES ($1,$2,$3,$4,$4)
         ON CONFLICT (scheduler_id) DO UPDATE SET scheduler_id = EXCLUDED.scheduler_id
         RETURNING ${schedulerColumns}`,
        [
          required(schedulerId, "schedulerId"),
          enabled,
          positive(intervalMs, "intervalMs"),
          now,
        ],
      ),
    read: async (schedulerId) => {
      const result = await options.client.query<SchedulerRow>(
        `SELECT ${schedulerColumns} FROM ${ns}.effect_reconciliation_scheduler WHERE scheduler_id = $1`,
        [required(schedulerId, "schedulerId")],
      );
      const row = result.rows[0];
      return row ? fromRow(row) : undefined;
    },
  };
};

export const createManagedEffectReconciliationScheduler = (options: {
  defaultPolicy: EffectReconciliationSchedulerPolicy;
  leaseMs: number;
  now?: () => number;
  onError?: (error: unknown) => void;
  pollMs: number;
  run: () => Promise<EffectReconciliationSchedulerResult>;
  schedulerId: string;
  store: EffectReconciliationSchedulerStore;
  workerId: string;
}) => {
  const now = options.now ?? Date.now;
  const schedulerId = required(options.schedulerId, "schedulerId");
  const workerId = required(options.workerId, "workerId");
  const leaseMs = positive(options.leaseMs, "leaseMs");
  const pollMs = positive(options.pollMs, "pollMs");
  positive(options.defaultPolicy.intervalMs, "defaultPolicy.intervalMs");
  let timer: ReturnType<typeof setInterval> | undefined;

  const initialize = () =>
    options.store.initialize({
      ...options.defaultPolicy,
      now: now(),
      schedulerId,
    });
  const runDueOnce = async () => {
    const startedAt = now();
    if (
      !(await options.store.claim({
        leaseMs,
        now: startedAt,
        owner: workerId,
        schedulerId,
      }))
    )
      return { ran: false } as const;
    try {
      const result = validateResult(await options.run());
      if (
        !(await options.store.complete({
          now: now(),
          owner: workerId,
          result,
          schedulerId,
        }))
      )
        throw new EffectReconciliationSchedulerError(
          "Reconciliation scheduler lease was lost before completion",
        );
      return { ran: true, result } as const;
    } catch (error) {
      await options.store.complete({
        errorCode: "run_failed",
        now: now(),
        owner: workerId,
        schedulerId,
      });
      options.onError?.(error);
      return { errorCode: "run_failed", ran: true } as const;
    }
  };

  return {
    configure: (policy: EffectReconciliationSchedulerPolicy) =>
      options.store.configure({
        enabled: policy.enabled,
        intervalMs: positive(policy.intervalMs, "intervalMs"),
        now: now(),
        schedulerId,
      }),
    drain: () => {
      if (timer) clearInterval(timer);
      timer = undefined;
    },
    initialize,
    posture: async () => ({
      active: timer !== undefined,
      policy: (await options.store.read(schedulerId)) ?? (await initialize()),
    }),
    runDueOnce,
    start: () => {
      timer ??= setInterval(
        () => void runDueOnce().catch((error) => options.onError?.(error)),
        pollMs,
      );
    },
  };
};
