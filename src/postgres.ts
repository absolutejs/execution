import type {
  EffectAttempt,
  EffectOutboxRecord,
  EffectRecord,
  EffectStore,
} from "./types";

export type ExecutionSqlResult<Row> = { rows: Row[] };
export type ExecutionSqlClient = {
  query: <Row = Record<string, unknown>>(
    text: string,
    values?: readonly unknown[],
  ) => Promise<ExecutionSqlResult<Row>>;
};

const namespaceOf = (namespace: string) => {
  if (!/^[a-z_][a-z0-9_]*$/.test(namespace)) {
    throw new Error("Execution namespace must be a simple identifier");
  }
  return namespace;
};

export const executionPostgresSchemaSql = (namespace = "execution") => {
  const ns = namespaceOf(namespace);
  return `CREATE SCHEMA IF NOT EXISTS ${ns};
CREATE TABLE IF NOT EXISTS ${ns}.effects (
  effect_id text PRIMARY KEY,
  action_id text NOT NULL,
  handler text NOT NULL,
  idempotency_key text NOT NULL UNIQUE,
  status text NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  available_at bigint NOT NULL,
  lease_owner text,
  lease_expires_at bigint,
  input_digest text NOT NULL,
  data jsonb NOT NULL,
  created_at bigint NOT NULL,
  updated_at bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS effects_claim_idx ON ${ns}.effects (status, available_at, lease_expires_at);
CREATE TABLE IF NOT EXISTS ${ns}.effect_attempts (
  attempt_id text PRIMARY KEY,
  effect_id text NOT NULL REFERENCES ${ns}.effects(effect_id) ON DELETE CASCADE,
  kind text NOT NULL,
  number integer NOT NULL,
  worker_id text NOT NULL,
  outcome text NOT NULL,
  error text,
  started_at bigint NOT NULL,
  finished_at bigint
);
CREATE INDEX IF NOT EXISTS effect_attempts_effect_idx ON ${ns}.effect_attempts (effect_id, started_at);
CREATE TABLE IF NOT EXISTS ${ns}.effect_outbox (
  event_id text PRIMARY KEY,
  effect_id text NOT NULL REFERENCES ${ns}.effects(effect_id) ON DELETE CASCADE,
  attempts integer NOT NULL DEFAULT 0,
  lease_owner text,
  lease_expires_at bigint,
  created_at bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS effect_outbox_claim_idx ON ${ns}.effect_outbox (lease_expires_at, created_at);`;
};

type EffectRow = {
  attempts: number;
  data: EffectRecord | string;
};

const parseEffect = (row: EffectRow | undefined) => {
  if (!row) return undefined;
  const data = typeof row.data === "string" ? JSON.parse(row.data) : row.data;
  return { ...data, attempts: row.attempts } as EffectRecord;
};

export const createPostgresEffectStore = ({
  client,
  namespace = "execution",
}: {
  client: ExecutionSqlClient;
  namespace?: string;
}): EffectStore => {
  const ns = namespaceOf(namespace);
  const effectUpdate = async (
    effectId: string,
    workerId: string,
    update: Record<string, unknown>,
    status: string,
    now: number,
  ) => {
    const dataUpdate = {
      ...update,
      status,
      updatedAt: now,
    };
    const result = await client.query<{ effect_id: string }>(
      `UPDATE ${ns}.effects SET status = $3, data = (data - 'leaseOwner' - 'leaseExpiresAt') || $4::jsonb, lease_owner = NULL, lease_expires_at = NULL, updated_at = $5, available_at = COALESCE($6, available_at) WHERE effect_id = $1 AND lease_owner = $2 AND status = 'leased' RETURNING effect_id`,
      [
        effectId,
        workerId,
        status,
        JSON.stringify(dataUpdate),
        now,
        update.availableAt ?? null,
      ],
    );
    return result.rows[0] !== undefined;
  };

  return {
    enqueue: async (effect) => {
      const result = await client.query<{ effect_id: string }>(
        `WITH inserted AS (
          INSERT INTO ${ns}.effects (effect_id, action_id, handler, idempotency_key, status, attempts, available_at, input_digest, data, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11)
          ON CONFLICT DO NOTHING RETURNING effect_id, created_at
        ), queued AS (
          INSERT INTO ${ns}.effect_outbox (event_id, effect_id, created_at)
          SELECT 'effect:' || effect_id, effect_id, created_at FROM inserted
        ) SELECT effect_id FROM inserted`,
        [
          effect.effectId,
          effect.actionId,
          effect.handler,
          effect.idempotencyKey,
          effect.status,
          effect.attempts,
          effect.availableAt,
          effect.inputDigest,
          JSON.stringify(effect),
          effect.createdAt,
          effect.updatedAt,
        ],
      );
      return result.rows[0] !== undefined;
    },
    claim: async (workerId, leaseMs, now) => {
      const result = await client.query<EffectRow>(
        `WITH candidate AS (
          SELECT effect_id FROM ${ns}.effects
          WHERE available_at <= $1 AND (status IN ('pending','failed') OR (status = 'leased' AND lease_expires_at <= $1))
          ORDER BY available_at, created_at FOR UPDATE SKIP LOCKED LIMIT 1
        ) UPDATE ${ns}.effects e SET status = 'leased', attempts = e.attempts + 1, lease_owner = $2, lease_expires_at = $1 + $3, updated_at = $1,
          data = e.data || jsonb_build_object('status','leased','attempts',e.attempts + 1,'leaseOwner',$2,'leaseExpiresAt',$1 + $3,'updatedAt',$1)
          FROM candidate WHERE e.effect_id = candidate.effect_id RETURNING e.data, e.attempts`,
        [now, workerId, leaseMs],
      );
      return parseEffect(result.rows[0]);
    },
    claimEffect: async (effectId, workerId, leaseMs, now) => {
      const result = await client.query<EffectRow>(
        `UPDATE ${ns}.effects SET status = 'leased', attempts = attempts + 1, lease_owner = $2, lease_expires_at = $3 + $4, updated_at = $3,
          data = data || jsonb_build_object('status','leased','attempts',attempts + 1,'leaseOwner',$2,'leaseExpiresAt',$3 + $4,'updatedAt',$3)
         WHERE effect_id = $1 AND available_at <= $3 AND (status IN ('pending','failed') OR (status = 'leased' AND lease_expires_at <= $3))
         RETURNING data, attempts`,
        [effectId, workerId, now, leaseMs],
      );
      return parseEffect(result.rows[0]);
    },
    claimOutbox: async (workerId, leaseMs, now) => {
      const result = await client.query<{
        attempts: number;
        effect_id: string;
        event_id: string;
        lease_expires_at: string;
        lease_owner: string;
      }>(
        `WITH candidate AS (
          SELECT event_id FROM ${ns}.effect_outbox
          WHERE lease_owner IS NULL OR lease_expires_at <= $1
          ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1
        ) UPDATE ${ns}.effect_outbox o SET attempts = o.attempts + 1, lease_owner = $2, lease_expires_at = $1 + $3
          FROM candidate WHERE o.event_id = candidate.event_id
          RETURNING o.event_id, o.effect_id, o.attempts, o.lease_owner, o.lease_expires_at`,
        [now, workerId, leaseMs],
      );
      const row = result.rows[0];
      return row
        ? {
            attempts: row.attempts,
            effectId: row.effect_id,
            eventId: row.event_id,
            leaseExpiresAt: Number(row.lease_expires_at),
            leaseOwner: row.lease_owner,
          }
        : undefined;
    },
    fail: (effectId, workerId, update, now) =>
      effectUpdate(effectId, workerId, update, update.status, now),
    finishAttempt: async (attemptId, outcome, now, error) => {
      await client.query(
        `UPDATE ${ns}.effect_attempts SET outcome = $2, finished_at = $3, error = $4 WHERE attempt_id = $1 AND outcome = 'running'`,
        [attemptId, outcome, now, error ?? null],
      );
    },
    finishCompensation: async (effectId, workerId, now, error) => {
      const result = await client.query<{ effect_id: string }>(
        `UPDATE ${ns}.effects SET status = $3, lease_owner = NULL, updated_at = $4, data = (data - 'leaseOwner') || $5::jsonb WHERE effect_id = $1 AND lease_owner = $2 AND status = 'compensating' RETURNING effect_id`,
        [
          effectId,
          workerId,
          error === undefined ? "compensated" : "compensation_failed",
          now,
          JSON.stringify({
            ...(error === undefined ? {} : { error }),
            status: error === undefined ? "compensated" : "compensation_failed",
            updatedAt: now,
          }),
        ],
      );
      return result.rows[0] !== undefined;
    },
    get: async (effectId) =>
      parseEffect(
        (
          await client.query<EffectRow>(
            `SELECT data, attempts FROM ${ns}.effects WHERE effect_id = $1`,
            [effectId],
          )
        ).rows[0],
      ),
    heartbeat: async (effectId, workerId, leaseMs, now) => {
      const result = await client.query<{ effect_id: string }>(
        `UPDATE ${ns}.effects SET lease_expires_at = $3 + $4, updated_at = $3, data = data || jsonb_build_object('leaseExpiresAt',$3 + $4,'updatedAt',$3) WHERE effect_id = $1 AND lease_owner = $2 AND status = 'leased' RETURNING effect_id`,
        [effectId, workerId, now, leaseMs],
      );
      return result.rows[0] !== undefined;
    },
    listAttempts: async (effectId) => {
      const result = await client.query<{
        attempt_id: string;
        effect_id: string;
        error: string | null;
        finished_at: string | null;
        kind: EffectAttempt["kind"];
        number: number;
        outcome: EffectAttempt["outcome"];
        started_at: string;
        worker_id: string;
      }>(
        `SELECT attempt_id, effect_id, kind, number, worker_id, outcome, error, started_at, finished_at FROM ${ns}.effect_attempts WHERE effect_id = $1 ORDER BY started_at`,
        [effectId],
      );
      return result.rows.map((row) => ({
        attemptId: row.attempt_id,
        effectId: row.effect_id,
        ...(row.error === null ? {} : { error: row.error }),
        ...(row.finished_at === null
          ? {}
          : { finishedAt: Number(row.finished_at) }),
        kind: row.kind,
        number: row.number,
        outcome: row.outcome,
        startedAt: Number(row.started_at),
        workerId: row.worker_id,
      }));
    },
    publishOutbox: async (eventId, workerId) => {
      const result = await client.query<{ event_id: string }>(
        `DELETE FROM ${ns}.effect_outbox WHERE event_id = $1 AND lease_owner = $2 RETURNING event_id`,
        [eventId, workerId],
      );
      return result.rows[0] !== undefined;
    },
    reconcile: async (effectId, update, now) => {
      const result = await client.query<{ effect_id: string }>(
        `UPDATE ${ns}.effects SET status = $2, data = data || $3::jsonb, updated_at = $4 WHERE effect_id = $1 AND status = 'unknown' RETURNING effect_id`,
        [
          effectId,
          update.status,
          JSON.stringify({ ...update, updatedAt: now }),
          now,
        ],
      );
      return result.rows[0] !== undefined;
    },
    recordAttempt: async (attempt: EffectAttempt) => {
      await client.query(
        `INSERT INTO ${ns}.effect_attempts (attempt_id, effect_id, kind, number, worker_id, outcome, error, started_at, finished_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (attempt_id) DO NOTHING`,
        [
          attempt.attemptId,
          attempt.effectId,
          attempt.kind,
          attempt.number,
          attempt.workerId,
          attempt.outcome,
          attempt.error ?? null,
          attempt.startedAt,
          attempt.finishedAt ?? null,
        ],
      );
    },
    retryOutbox: async (eventId, workerId) => {
      const result = await client.query<{ event_id: string }>(
        `UPDATE ${ns}.effect_outbox SET lease_owner = NULL, lease_expires_at = NULL WHERE event_id = $1 AND lease_owner = $2 RETURNING event_id`,
        [eventId, workerId],
      );
      return result.rows[0] !== undefined;
    },
    startCompensation: async (effectId, workerId, now) => {
      const result = await client.query<EffectRow>(
        `UPDATE ${ns}.effects SET status = 'compensating', lease_owner = $2, updated_at = $3, data = data || jsonb_build_object('status','compensating','leaseOwner',$2,'updatedAt',$3) WHERE effect_id = $1 AND status IN ('succeeded','compensation_failed') RETURNING data, attempts`,
        [effectId, workerId, now],
      );
      return parseEffect(result.rows[0]);
    },
    succeed: (effectId, workerId, result, now) =>
      effectUpdate(effectId, workerId, { result }, "succeeded", now),
  };
};
