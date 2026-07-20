import type { ExecutionSqlClient } from "./postgres";

export type EffectAdapterDestination = {
  kind:
    | "custom"
    | "https-origin"
    | "merchant"
    | "message-recipient"
    | "storage";
  value: string;
};

export type EffectAdapterDescriptor = {
  adapterId: string;
  compensation: { supported: boolean };
  credentialBindings: ReadonlyArray<{
    alias: string;
    destination: string;
    mode: "http-header" | "provider-sdk" | "request-field";
  }>;
  destinations: ReadonlyArray<EffectAdapterDestination>;
  effects: ReadonlyArray<string>;
  idempotency: { scope: "effect" | "tenant-effect"; supported: boolean };
  reconciliation: { mode: "manual" | "query" | "webhook" | "unsupported" };
  spendAuthority: {
    canSpend: boolean;
    currencies: ReadonlyArray<string>;
    requiresMandate: boolean;
  };
  title: string;
  version: string;
};

export type EffectAdapterConformanceCertificate = {
  digest: string;
  issuedAt: string;
  passed: boolean;
  profile: string;
  proof?: unknown;
  reports: ReadonlyArray<{
    failed: number;
    passed: number;
    results: ReadonlyArray<{ error?: string; name: string; passed: boolean }>;
    suite: string;
  }>;
  subject: { name: string; version: string };
};

export type EffectAdapterCertification = {
  adapterId: string;
  adapterVersion: string;
  certificate: EffectAdapterConformanceCertificate;
  descriptorDigest: string;
  evidenceReference: string;
};

export type EffectAdapterRegistryRecord = {
  active: boolean;
  certification?: EffectAdapterCertification;
  descriptor: EffectAdapterDescriptor;
  descriptorDigest: string;
  registeredAt: number;
  updatedAt: number;
};

export type EffectAdapterRegistryStore = {
  get: (adapterId: string) => Promise<EffectAdapterRegistryRecord | undefined>;
  list: () => Promise<EffectAdapterRegistryRecord[]>;
  save: (record: EffectAdapterRegistryRecord) => Promise<void>;
};

export type EffectAdapterRegistryPosture = EffectAdapterRegistryRecord & {
  eligible: boolean;
  reasons: string[];
};

export class EffectAdapterActivationError extends Error {}

const DEFAULT_CERTIFICATE_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1_000;
const REQUIRED_PROFILE = "absolutejs-agent-first-1";
const REQUIRED_SUITE = "agent-durable-execution-boundary";

const stable = (value: unknown): string =>
  JSON.stringify(value, (_key, item) =>
    item && typeof item === "object" && !Array.isArray(item)
      ? Object.fromEntries(
          Object.entries(item).sort(([a], [b]) => a.localeCompare(b)),
        )
      : item,
  );

export const effectAdapterDescriptorDigest = async (
  descriptor: EffectAdapterDescriptor,
) =>
  `sha256:${Buffer.from(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(stable(descriptor)))).toString("hex")}`;

const conformanceCertificateDigest = async (
  certificate: EffectAdapterConformanceCertificate,
) =>
  `sha256:${Buffer.from(
    await crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(
        stable({
          subject: certificate.subject,
          profile: certificate.profile,
          issuedAt: certificate.issuedAt,
          passed: certificate.passed,
          reports: certificate.reports,
        }),
      ),
    ),
  ).toString("hex")}`;

const duplicates = (values: ReadonlyArray<string>) =>
  new Set(values).size !== values.length;

const validateDescriptor = (descriptor: EffectAdapterDescriptor) => {
  if (!descriptor.adapterId.trim() || !descriptor.version.trim())
    throw new Error("Effect adapter identity and version are required");
  if (descriptor.effects.length === 0 || duplicates(descriptor.effects))
    throw new Error("Effect adapter effects must be non-empty and unique");
  if (duplicates(descriptor.destinations.map(({ value }) => value)))
    throw new Error("Effect adapter destinations must be unique");
  const destinations = new Set(
    descriptor.destinations.map(({ value }) => value),
  );
  if (
    descriptor.credentialBindings.some(
      ({ destination }) => !destinations.has(destination),
    )
  )
    throw new Error("Credential binding targets an undeclared destination");
  if (
    descriptor.spendAuthority.canSpend &&
    (!descriptor.spendAuthority.requiresMandate ||
      descriptor.spendAuthority.currencies.length === 0)
  )
    throw new Error("Spending adapters require a mandate and currencies");
  if (
    !descriptor.spendAuthority.canSpend &&
    descriptor.spendAuthority.currencies.length > 0
  )
    throw new Error("Non-spending adapters cannot declare currencies");
};

export const createMemoryEffectAdapterRegistryStore =
  (): EffectAdapterRegistryStore => {
    const records = new Map<string, EffectAdapterRegistryRecord>();
    return {
      get: async (adapterId) => records.get(adapterId),
      list: async () => [...records.values()],
      save: async (record) => {
        records.set(record.descriptor.adapterId, structuredClone(record));
      },
    };
  };

const parseRecord = (row: {
  active: boolean;
  certification: EffectAdapterCertification | string | null;
  descriptor: EffectAdapterDescriptor | string;
  descriptor_digest: string;
  registered_at: number | string;
  updated_at: number | string;
}): EffectAdapterRegistryRecord => ({
  active: row.active,
  ...(row.certification
    ? {
        certification:
          typeof row.certification === "string"
            ? JSON.parse(row.certification)
            : row.certification,
      }
    : {}),
  descriptor:
    typeof row.descriptor === "string"
      ? JSON.parse(row.descriptor)
      : row.descriptor,
  descriptorDigest: row.descriptor_digest,
  registeredAt: Number(row.registered_at),
  updatedAt: Number(row.updated_at),
});

export const effectAdapterRegistryPostgresSchemaSql = (
  namespace = "execution",
) => {
  if (!/^[a-z_][a-z0-9_]*$/.test(namespace))
    throw new Error("Execution namespace must be a simple identifier");
  return `CREATE TABLE IF NOT EXISTS ${namespace}.adapter_registry (
  adapter_id text PRIMARY KEY,
  version text NOT NULL,
  descriptor_digest text NOT NULL,
  descriptor jsonb NOT NULL,
  certification jsonb,
  active boolean NOT NULL DEFAULT false,
  registered_at bigint NOT NULL,
  updated_at bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS adapter_registry_active_idx ON ${namespace}.adapter_registry (active, adapter_id);`;
};

export const createPostgresEffectAdapterRegistryStore = (options: {
  client: ExecutionSqlClient;
  namespace?: string;
}): EffectAdapterRegistryStore => {
  const namespace = options.namespace ?? "execution";
  if (!/^[a-z_][a-z0-9_]*$/.test(namespace))
    throw new Error("Execution namespace must be a simple identifier");
  const columns =
    "active, certification, descriptor, descriptor_digest, registered_at, updated_at";
  return {
    get: async (adapterId) => {
      const result = await options.client.query(
        `SELECT ${columns} FROM ${namespace}.adapter_registry WHERE adapter_id = $1`,
        [adapterId],
      );
      const row = result.rows[0];
      return row
        ? parseRecord(row as Parameters<typeof parseRecord>[0])
        : undefined;
    },
    list: async () => {
      const result = await options.client.query(
        `SELECT ${columns} FROM ${namespace}.adapter_registry ORDER BY adapter_id`,
      );
      return result.rows.map((row) =>
        parseRecord(row as Parameters<typeof parseRecord>[0]),
      );
    },
    save: async (record) => {
      await options.client.query(
        `INSERT INTO ${namespace}.adapter_registry
          (adapter_id, version, descriptor_digest, descriptor, certification, active, registered_at, updated_at)
         VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7, $8)
         ON CONFLICT (adapter_id) DO UPDATE SET
          version = excluded.version,
          descriptor_digest = excluded.descriptor_digest,
          descriptor = excluded.descriptor,
          certification = excluded.certification,
          active = excluded.active,
          registered_at = excluded.registered_at,
          updated_at = excluded.updated_at`,
        [
          record.descriptor.adapterId,
          record.descriptor.version,
          record.descriptorDigest,
          JSON.stringify(record.descriptor),
          record.certification ? JSON.stringify(record.certification) : null,
          record.active,
          record.registeredAt,
          record.updatedAt,
        ],
      );
    },
  };
};

export const createEffectAdapterRegistry = (options: {
  certificateMaxAgeMs?: number;
  now?: () => number;
  store: EffectAdapterRegistryStore;
  verifyEvidence: (
    certification: EffectAdapterCertification,
  ) => boolean | Promise<boolean>;
}) => {
  const now = options.now ?? Date.now;
  const maximumAge =
    options.certificateMaxAgeMs ?? DEFAULT_CERTIFICATE_MAX_AGE_MS;
  if (!Number.isFinite(maximumAge) || maximumAge <= 0)
    throw new Error("Certificate maximum age must be positive");

  const posture = async (
    record: EffectAdapterRegistryRecord,
  ): Promise<EffectAdapterRegistryPosture> => {
    const reasons: string[] = [];
    const certification = record.certification;
    if (!certification) reasons.push("certification_missing");
    if (certification) {
      const issuedAt = Date.parse(certification.certificate.issuedAt);
      if (
        certification.adapterId !== record.descriptor.adapterId ||
        certification.adapterVersion !== record.descriptor.version ||
        certification.descriptorDigest !== record.descriptorDigest ||
        certification.certificate.subject.name !==
          record.descriptor.adapterId ||
        certification.certificate.subject.version !== record.descriptor.version
      )
        reasons.push("certification_identity_mismatch");
      if (
        certification.certificate.profile !== REQUIRED_PROFILE ||
        !certification.certificate.passed
      )
        reasons.push("certification_failed");
      if (
        certification.certificate.digest !==
        (await conformanceCertificateDigest(certification.certificate))
      )
        reasons.push("certification_digest_invalid");
      if (
        !Number.isFinite(issuedAt) ||
        issuedAt > now() ||
        now() - issuedAt > maximumAge
      )
        reasons.push("certification_stale");
      const suite = certification.certificate.reports.find(
        ({ suite }) => suite === REQUIRED_SUITE,
      );
      if (
        !suite ||
        suite.failed !== 0 ||
        suite.results.some(({ passed }) => !passed)
      )
        reasons.push("required_suite_missing_or_failed");
      if (!(await options.verifyEvidence(certification)))
        reasons.push("evidence_unverified");
      if (!certification.evidenceReference.trim())
        reasons.push("evidence_reference_missing");
    }
    return { ...record, eligible: reasons.length === 0, reasons };
  };

  const requireEligible = async (adapterId: string) => {
    const record = await options.store.get(adapterId);
    if (!record)
      throw new EffectAdapterActivationError("Effect adapter not found");
    const evaluated = await posture(record);
    if (!evaluated.eligible)
      throw new EffectAdapterActivationError(
        `Effect adapter is not certifiable: ${evaluated.reasons.join(", ")}`,
      );
    return evaluated;
  };

  return {
    activate: async (adapterId: string) => {
      const {
        eligible: _eligible,
        reasons: _reasons,
        ...record
      } = await requireEligible(adapterId);
      await options.store.save({ ...record, active: true, updatedAt: now() });
    },
    authorize: async (adapterId: string, effect: string) => {
      const record = await requireEligible(adapterId);
      if (!record.active)
        throw new EffectAdapterActivationError("Effect adapter is inactive");
      if (!record.descriptor.effects.includes(effect))
        throw new EffectAdapterActivationError(
          "Effect is outside adapter scope",
        );
      return record.descriptor;
    },
    certify: async (certification: EffectAdapterCertification) => {
      const record = await options.store.get(certification.adapterId);
      if (!record)
        throw new EffectAdapterActivationError("Effect adapter not found");
      const candidate = { ...record, certification, updatedAt: now() };
      const evaluated = await posture(candidate);
      if (!evaluated.eligible)
        throw new EffectAdapterActivationError(
          `Effect adapter certification rejected: ${evaluated.reasons.join(", ")}`,
        );
      await options.store.save(candidate);
    },
    deactivate: async (adapterId: string) => {
      const record = await options.store.get(adapterId);
      if (!record)
        throw new EffectAdapterActivationError("Effect adapter not found");
      await options.store.save({ ...record, active: false, updatedAt: now() });
    },
    inventory: async () =>
      Promise.all((await options.store.list()).map(posture)),
    register: async (descriptor: EffectAdapterDescriptor) => {
      validateDescriptor(descriptor);
      const descriptorDigest = await effectAdapterDescriptorDigest(descriptor);
      const existing = await options.store.get(descriptor.adapterId);
      const changed = existing?.descriptorDigest !== descriptorDigest;
      const timestamp = now();
      await options.store.save({
        active: changed ? false : (existing?.active ?? false),
        ...(!changed && existing?.certification
          ? { certification: existing.certification }
          : {}),
        descriptor,
        descriptorDigest,
        registeredAt: existing?.registeredAt ?? timestamp,
        updatedAt: timestamp,
      });
      return descriptorDigest;
    },
  };
};
