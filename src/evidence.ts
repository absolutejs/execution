import type { ExecutionSqlClient } from "./postgres";

export type EffectEvidenceOutcome = "confirmed_succeeded" | "dead_letter";

export type EffectEvidenceRecord = {
  deliveryId: string;
  effectId: string;
  eventType: string;
  evidenceReference: string;
  occurredAt: number;
  outcome: EffectEvidenceOutcome;
  provider: string;
  providerResourceId?: string;
  receivedAt: number;
  tenantId: string;
  verifier: string;
};

export type EffectEvidenceStore = {
  get: (
    provider: string,
    deliveryId: string,
  ) => Promise<EffectEvidenceRecord | undefined>;
  list: (input: {
    effectId?: string;
    limit: number;
    tenantId?: string;
  }) => Promise<EffectEvidenceRecord[]>;
  put: (evidence: EffectEvidenceRecord) => Promise<boolean>;
};

export class EffectEvidenceError extends Error {}

const required = (value: string, field: string) => {
  const normalized = value.trim();
  if (!normalized) throw new EffectEvidenceError(`${field} is required`);
  return normalized;
};

const validated = (evidence: EffectEvidenceRecord): EffectEvidenceRecord => ({
  ...evidence,
  deliveryId: required(evidence.deliveryId, "deliveryId"),
  effectId: required(evidence.effectId, "effectId"),
  eventType: required(evidence.eventType, "eventType"),
  evidenceReference: required(evidence.evidenceReference, "evidenceReference"),
  provider: required(evidence.provider, "provider"),
  tenantId: required(evidence.tenantId, "tenantId"),
  verifier: required(evidence.verifier, "verifier"),
});

export const createEffectEvidenceIngestion = (options: {
  reconcile: (
    evidence: EffectEvidenceRecord,
  ) => Promise<"already_terminal" | "resolved">;
  store: EffectEvidenceStore;
}) => ({
  ingest: async (input: EffectEvidenceRecord) => {
    const evidence = validated(input);
    const inserted = await options.store.put(evidence);
    const retained = inserted
      ? evidence
      : await options.store.get(evidence.provider, evidence.deliveryId);
    if (!retained)
      throw new EffectEvidenceError("Retained effect evidence is unavailable");
    if (
      retained.effectId !== evidence.effectId ||
      retained.tenantId !== evidence.tenantId ||
      retained.evidenceReference !== evidence.evidenceReference
    )
      throw new EffectEvidenceError(
        "Duplicate evidence identity changed binding",
      );

    return {
      duplicate: !inserted,
      evidence: retained,
      reconciliation: await options.reconcile(retained),
    };
  },
});

export const createMemoryEffectEvidenceStore = (): EffectEvidenceStore => {
  const rows = new Map<string, EffectEvidenceRecord>();
  return {
    get: async (provider, deliveryId) => rows.get(`${provider}:${deliveryId}`),
    list: async ({ effectId, limit, tenantId }) =>
      [...rows.values()]
        .filter(
          (row) =>
            (!effectId || row.effectId === effectId) &&
            (!tenantId || row.tenantId === tenantId),
        )
        .sort((left, right) => right.receivedAt - left.receivedAt)
        .slice(0, limit)
        .map((row) => structuredClone(row)),
    put: async (evidence) => {
      const key = `${evidence.provider}:${evidence.deliveryId}`;
      if (rows.has(key)) return false;
      rows.set(key, structuredClone(evidence));
      return true;
    },
  };
};

const namespaceOf = (namespace: string) => {
  if (!/^[a-z_][a-z0-9_]*$/.test(namespace))
    throw new EffectEvidenceError(
      "Evidence namespace must be a simple identifier",
    );
  return namespace;
};

export const effectEvidencePostgresSchemaSql = (namespace = "execution") => {
  const ns = namespaceOf(namespace);
  return `CREATE TABLE IF NOT EXISTS ${ns}.effect_evidence (
  provider text NOT NULL,
  delivery_id text NOT NULL,
  effect_id text NOT NULL REFERENCES ${ns}.effects(effect_id) ON DELETE CASCADE,
  tenant_id text NOT NULL,
  event_type text NOT NULL,
  outcome text NOT NULL,
  evidence_reference text NOT NULL,
  provider_resource_id text,
  verifier text NOT NULL,
  occurred_at bigint NOT NULL,
  received_at bigint NOT NULL,
  PRIMARY KEY (provider, delivery_id)
);
CREATE INDEX IF NOT EXISTS effect_evidence_effect_idx ON ${ns}.effect_evidence (effect_id, received_at DESC);`;
};

type EvidenceRow = {
  delivery_id: string;
  effect_id: string;
  event_type: string;
  evidence_reference: string;
  occurred_at: string | number;
  outcome: EffectEvidenceOutcome;
  provider: string;
  provider_resource_id: string | null;
  received_at: string | number;
  tenant_id: string;
  verifier: string;
};
const fromRow = (row: EvidenceRow): EffectEvidenceRecord => ({
  deliveryId: row.delivery_id,
  effectId: row.effect_id,
  eventType: row.event_type,
  evidenceReference: row.evidence_reference,
  occurredAt: Number(row.occurred_at),
  outcome: row.outcome,
  provider: row.provider,
  ...(row.provider_resource_id
    ? { providerResourceId: row.provider_resource_id }
    : {}),
  receivedAt: Number(row.received_at),
  tenantId: row.tenant_id,
  verifier: row.verifier,
});

export const createPostgresEffectEvidenceStore = (options: {
  client: ExecutionSqlClient;
  namespace?: string;
}): EffectEvidenceStore => {
  const ns = namespaceOf(options.namespace ?? "execution");
  const select = `SELECT provider, delivery_id, effect_id, tenant_id, event_type, outcome, evidence_reference, provider_resource_id, verifier, occurred_at, received_at FROM ${ns}.effect_evidence`;
  return {
    get: async (provider, deliveryId) => {
      const result = await options.client.query<EvidenceRow>(
        `${select} WHERE provider = $1 AND delivery_id = $2`,
        [provider, deliveryId],
      );
      return result.rows[0] ? fromRow(result.rows[0]) : undefined;
    },
    list: async ({ effectId, limit, tenantId }) => {
      const result = await options.client.query<EvidenceRow>(
        `${select} WHERE ($1::text IS NULL OR tenant_id = $1) AND ($2::text IS NULL OR effect_id = $2) ORDER BY received_at DESC LIMIT $3`,
        [tenantId ?? null, effectId ?? null, limit],
      );
      return result.rows.map(fromRow);
    },
    put: async (evidence) => {
      const result = await options.client.query<{ delivery_id: string }>(
        `INSERT INTO ${ns}.effect_evidence (provider, delivery_id, effect_id, tenant_id, event_type, outcome, evidence_reference, provider_resource_id, verifier, occurred_at, received_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (provider, delivery_id) DO NOTHING RETURNING delivery_id`,
        [
          evidence.provider,
          evidence.deliveryId,
          evidence.effectId,
          evidence.tenantId,
          evidence.eventType,
          evidence.outcome,
          evidence.evidenceReference,
          evidence.providerResourceId ?? null,
          evidence.verifier,
          evidence.occurredAt,
          evidence.receivedAt,
        ],
      );
      return result.rows[0] !== undefined;
    },
  };
};
