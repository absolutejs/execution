import type {
  EffectAdapterDescriptor,
  EffectAdapterRegistryPosture,
} from "./adapterRegistry";
import type { ExecutionSqlClient } from "./postgres";

export type EffectAdapterCredentialInstallation = {
  adapterAlias: string;
  destination: string;
  secretAlias: string;
};

export type EffectAdapterInstallationPolicy = {
  credentials: ReadonlyArray<EffectAdapterCredentialInstallation>;
  destinations: ReadonlyArray<string>;
  effects: ReadonlyArray<string>;
  spend: {
    currency: string | null;
    mandateId: string | null;
    maxMinorPerEffect: number;
  };
};

export type EffectAdapterInstallationRecord = {
  adapterId: string;
  adapterVersion: string;
  descriptorDigest: string;
  enabled: boolean;
  installationId: string;
  installedAt: number;
  policy: EffectAdapterInstallationPolicy;
  tenantId: string;
  updatedAt: number;
};

export type EffectAdapterInstallationPosture =
  EffectAdapterInstallationRecord & {
    ready: boolean;
    reasons: string[];
  };

export type EffectAdapterInstallationStore = {
  get: (
    tenantId: string,
    installationId: string,
  ) => Promise<EffectAdapterInstallationRecord | undefined>;
  list: (input?: {
    tenantId?: string;
  }) => Promise<EffectAdapterInstallationRecord[]>;
  save: (record: EffectAdapterInstallationRecord) => Promise<void>;
};

export type EffectAdapterInstallationRegistry = {
  authorize: (input: EffectAdapterInstallationAuthorization) => Promise<{
    adapter: EffectAdapterDescriptor;
    credentials: ReadonlyArray<EffectAdapterCredentialInstallation>;
    installation: EffectAdapterInstallationRecord;
  }>;
  disable: (tenantId: string, installationId: string) => Promise<void>;
  enable: (tenantId: string, installationId: string) => Promise<void>;
  inventory: (input?: {
    tenantId?: string;
  }) => Promise<EffectAdapterInstallationPosture[]>;
  put: (
    input: EffectAdapterInstallationInput,
  ) => Promise<EffectAdapterInstallationRecord>;
};

export type EffectAdapterInstallationAuthorization = {
  destination?: string;
  effect: string;
  installationId: string;
  spendMinor?: number;
  tenantId: string;
};

export type EffectAdapterInstallationInput = {
  adapterId: string;
  installationId: string;
  policy: EffectAdapterInstallationPolicy;
  tenantId: string;
};

export class EffectAdapterInstallationError extends Error {}

type AdapterRegistry = {
  authorize: (
    adapterId: string,
    effect: string,
  ) => Promise<EffectAdapterDescriptor>;
  inventory: () => Promise<EffectAdapterRegistryPosture[]>;
};

const simpleIdentifier = (value: string) => /^[a-z_][a-z0-9_]*$/.test(value);
const duplicates = (values: ReadonlyArray<string>) =>
  new Set(values).size !== values.length;
const credentialKey = (input: { adapterAlias: string; destination: string }) =>
  `${input.destination}\u0000${input.adapterAlias}`;

const validateIdentity = (input: EffectAdapterInstallationInput) => {
  if (
    !input.installationId.trim() ||
    !input.tenantId.trim() ||
    !input.adapterId.trim()
  )
    throw new EffectAdapterInstallationError(
      "Installation, tenant, and adapter identities are required",
    );
};

const validatePolicy = (
  descriptor: EffectAdapterDescriptor,
  policy: EffectAdapterInstallationPolicy,
) => {
  if (policy.effects.length === 0 || duplicates(policy.effects))
    throw new EffectAdapterInstallationError(
      "Installation effects must be non-empty and unique",
    );
  if (policy.effects.some((effect) => !descriptor.effects.includes(effect)))
    throw new EffectAdapterInstallationError(
      "Installation effect is outside the adapter descriptor",
    );
  if (duplicates(policy.destinations))
    throw new EffectAdapterInstallationError(
      "Installation destinations must be unique",
    );
  const declaredDestinations = new Set(
    descriptor.destinations.map(({ value }) => value),
  );
  if (
    policy.destinations.some(
      (destination) => !declaredDestinations.has(destination),
    )
  )
    throw new EffectAdapterInstallationError(
      "Installation destination is outside the adapter descriptor",
    );
  if (descriptor.destinations.length > 0 && policy.destinations.length === 0)
    throw new EffectAdapterInstallationError(
      "Installation must allow at least one declared destination",
    );
  const allowedDestinations = new Set(policy.destinations);
  const requiredCredentials = new Set(
    descriptor.credentialBindings
      .filter(({ destination }) => allowedDestinations.has(destination))
      .map(({ alias, destination }) =>
        credentialKey({ adapterAlias: alias, destination }),
      ),
  );
  const installedCredentials = policy.credentials.map(credentialKey);
  if (
    duplicates(installedCredentials) ||
    policy.credentials.some(
      ({ adapterAlias, destination, secretAlias }) =>
        !adapterAlias.trim() ||
        !destination.trim() ||
        !secretAlias.trim() ||
        !requiredCredentials.has(credentialKey({ adapterAlias, destination })),
    ) ||
    installedCredentials.length !== requiredCredentials.size
  )
    throw new EffectAdapterInstallationError(
      "Installation must bind every required credential exactly once",
    );
  const { currency, mandateId, maxMinorPerEffect } = policy.spend;
  if (!Number.isSafeInteger(maxMinorPerEffect) || maxMinorPerEffect < 0)
    throw new EffectAdapterInstallationError(
      "Installation spend ceiling must be a non-negative safe integer",
    );
  if (!descriptor.spendAuthority.canSpend) {
    if (maxMinorPerEffect !== 0 || currency !== null || mandateId !== null)
      throw new EffectAdapterInstallationError(
        "Non-spending adapters cannot receive spend authority",
      );
    return;
  }
  if (maxMinorPerEffect === 0) {
    if (currency !== null || mandateId !== null)
      throw new EffectAdapterInstallationError(
        "Zero spend authority cannot bind currency or mandate",
      );
    return;
  }
  if (!currency || !descriptor.spendAuthority.currencies.includes(currency))
    throw new EffectAdapterInstallationError(
      "Installation spend currency is outside the adapter descriptor",
    );
  if (descriptor.spendAuthority.requiresMandate && !mandateId?.trim())
    throw new EffectAdapterInstallationError(
      "Installation spend authority requires a mandate",
    );
  if (policy.effects.length !== 1 || policy.destinations.length > 1)
    throw new EffectAdapterInstallationError(
      "Spending installations require one exact effect and destination scope",
    );
};

export const createMemoryEffectAdapterInstallationStore =
  (): EffectAdapterInstallationStore => {
    const records = new Map<string, EffectAdapterInstallationRecord>();
    return {
      get: async (tenantId, installationId) => {
        const record = records.get(installationId);
        return record?.tenantId === tenantId ? record : undefined;
      },
      list: async (input) =>
        [...records.values()].filter(
          ({ tenantId }) => !input?.tenantId || tenantId === input.tenantId,
        ),
      save: async (record) => {
        const existing = records.get(record.installationId);
        if (existing && existing.tenantId !== record.tenantId)
          throw new EffectAdapterInstallationError(
            "Installation identity belongs to another tenant",
          );
        records.set(record.installationId, structuredClone(record));
      },
    };
  };

const parseInstallation = (row: {
  adapter_id: string;
  adapter_version: string;
  descriptor_digest: string;
  enabled: boolean;
  installation_id: string;
  installed_at: number | string;
  policy: EffectAdapterInstallationPolicy | string;
  tenant_id: string;
  updated_at: number | string;
}): EffectAdapterInstallationRecord => ({
  adapterId: row.adapter_id,
  adapterVersion: row.adapter_version,
  descriptorDigest: row.descriptor_digest,
  enabled: row.enabled,
  installationId: row.installation_id,
  installedAt: Number(row.installed_at),
  policy: typeof row.policy === "string" ? JSON.parse(row.policy) : row.policy,
  tenantId: row.tenant_id,
  updatedAt: Number(row.updated_at),
});

export const effectAdapterInstallationsPostgresSchemaSql = (
  namespace = "execution",
) => {
  if (!simpleIdentifier(namespace))
    throw new Error("Execution namespace must be a simple identifier");
  return `CREATE TABLE IF NOT EXISTS ${namespace}.adapter_installations (
  installation_id text PRIMARY KEY,
  tenant_id text NOT NULL,
  adapter_id text NOT NULL,
  adapter_version text NOT NULL,
  descriptor_digest text NOT NULL,
  policy jsonb NOT NULL,
  enabled boolean NOT NULL DEFAULT false,
  installed_at bigint NOT NULL,
  updated_at bigint NOT NULL,
  UNIQUE (tenant_id, adapter_id)
);
CREATE INDEX IF NOT EXISTS adapter_installations_tenant_idx ON ${namespace}.adapter_installations (tenant_id, installation_id);
CREATE INDEX IF NOT EXISTS adapter_installations_enabled_idx ON ${namespace}.adapter_installations (enabled, adapter_id);`;
};

export const createPostgresEffectAdapterInstallationStore = (options: {
  client: ExecutionSqlClient;
  namespace?: string;
}): EffectAdapterInstallationStore => {
  const namespace = options.namespace ?? "execution";
  if (!simpleIdentifier(namespace))
    throw new Error("Execution namespace must be a simple identifier");
  const columns =
    "adapter_id, adapter_version, descriptor_digest, enabled, installation_id, installed_at, policy, tenant_id, updated_at";
  return {
    get: async (tenantId, installationId) => {
      const result = await options.client.query(
        `SELECT ${columns} FROM ${namespace}.adapter_installations WHERE tenant_id = $1 AND installation_id = $2`,
        [tenantId, installationId],
      );
      const row = result.rows[0];
      return row
        ? parseInstallation(row as Parameters<typeof parseInstallation>[0])
        : undefined;
    },
    list: async (input) => {
      const result = input?.tenantId
        ? await options.client.query(
            `SELECT ${columns} FROM ${namespace}.adapter_installations WHERE tenant_id = $1 ORDER BY adapter_id, installation_id`,
            [input.tenantId],
          )
        : await options.client.query(
            `SELECT ${columns} FROM ${namespace}.adapter_installations ORDER BY tenant_id, adapter_id, installation_id`,
          );
      return result.rows.map((row) =>
        parseInstallation(row as Parameters<typeof parseInstallation>[0]),
      );
    },
    save: async (record) => {
      const result = await options.client.query(
        `INSERT INTO ${namespace}.adapter_installations
          (installation_id, tenant_id, adapter_id, adapter_version, descriptor_digest, policy, enabled, installed_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9)
         ON CONFLICT (installation_id) DO UPDATE SET
          adapter_id = excluded.adapter_id,
          adapter_version = excluded.adapter_version,
          descriptor_digest = excluded.descriptor_digest,
          policy = excluded.policy,
          enabled = excluded.enabled,
          installed_at = excluded.installed_at,
          updated_at = excluded.updated_at
         WHERE ${namespace}.adapter_installations.tenant_id = excluded.tenant_id
         RETURNING installation_id`,
        [
          record.installationId,
          record.tenantId,
          record.adapterId,
          record.adapterVersion,
          record.descriptorDigest,
          JSON.stringify(record.policy),
          record.enabled,
          record.installedAt,
          record.updatedAt,
        ],
      );
      if (result.rows.length !== 1)
        throw new EffectAdapterInstallationError(
          "Installation identity belongs to another tenant",
        );
    },
  };
};

export const createEffectAdapterInstallationRegistry = (options: {
  adapters: AdapterRegistry;
  now?: () => number;
  store: EffectAdapterInstallationStore;
  verifyCredentialAlias: (
    tenantId: string,
    secretAlias: string,
  ) => boolean | Promise<boolean>;
  verifyMandate: (input: {
    adapterId: string;
    amountMinor: number;
    currency: string;
    destination?: string;
    effect: string;
    mandateId: string;
    tenantId: string;
  }) => boolean | Promise<boolean>;
}): EffectAdapterInstallationRegistry => {
  const now = options.now ?? Date.now;

  const adapterPosture = async (adapterId: string) =>
    (await options.adapters.inventory()).find(
      ({ descriptor }) => descriptor.adapterId === adapterId,
    );

  const evaluate = async (
    record: EffectAdapterInstallationRecord,
  ): Promise<EffectAdapterInstallationPosture> => {
    const reasons: string[] = [];
    const adapter = await adapterPosture(record.adapterId);
    if (!adapter) reasons.push("adapter_missing");
    if (adapter) {
      if (!adapter.active) reasons.push("adapter_inactive");
      if (!adapter.eligible) reasons.push("adapter_ineligible");
      if (
        adapter.descriptor.version !== record.adapterVersion ||
        adapter.descriptorDigest !== record.descriptorDigest
      )
        reasons.push("descriptor_pin_mismatch");
      try {
        validatePolicy(adapter.descriptor, record.policy);
      } catch {
        reasons.push("installation_policy_invalid");
      }
    }
    const credentialChecks = await Promise.all(
      record.policy.credentials.map(({ secretAlias }) =>
        options.verifyCredentialAlias(record.tenantId, secretAlias),
      ),
    );
    if (credentialChecks.some((available) => !available))
      reasons.push("credential_alias_unavailable");
    const { currency, mandateId, maxMinorPerEffect } = record.policy.spend;
    if (
      maxMinorPerEffect > 0 &&
      currency &&
      mandateId &&
      !(await options.verifyMandate({
        adapterId: record.adapterId,
        amountMinor: maxMinorPerEffect,
        currency,
        ...(record.policy.destinations[0]
          ? { destination: record.policy.destinations[0] }
          : {}),
        effect: record.policy.effects[0]!,
        mandateId,
        tenantId: record.tenantId,
      }))
    )
      reasons.push("mandate_unavailable");

    return { ...record, ready: reasons.length === 0, reasons };
  };

  const requireRecord = async (tenantId: string, installationId: string) => {
    const record = await options.store.get(tenantId, installationId);
    if (!record)
      throw new EffectAdapterInstallationError("Installation not found");
    return record;
  };

  const requireReady = async (tenantId: string, installationId: string) => {
    const posture = await evaluate(
      await requireRecord(tenantId, installationId),
    );
    if (!posture.ready)
      throw new EffectAdapterInstallationError(
        `Installation is not ready: ${posture.reasons.join(", ")}`,
      );
    return posture;
  };

  return {
    authorize: async (input) => {
      const record = await requireReady(input.tenantId, input.installationId);
      if (!record.enabled)
        throw new EffectAdapterInstallationError("Installation is disabled");
      if (!record.policy.effects.includes(input.effect))
        throw new EffectAdapterInstallationError(
          "Effect is outside installation scope",
        );
      const adapter = await options.adapters.authorize(
        record.adapterId,
        input.effect,
      );
      if (adapter.destinations.length > 0 && !input.destination)
        throw new EffectAdapterInstallationError(
          "Effect destination is required",
        );
      if (
        input.destination &&
        !record.policy.destinations.includes(input.destination)
      )
        throw new EffectAdapterInstallationError(
          "Destination is outside installation scope",
        );
      const spendMinor = input.spendMinor ?? 0;
      if (!Number.isSafeInteger(spendMinor) || spendMinor < 0)
        throw new EffectAdapterInstallationError(
          "Effect spend must be a non-negative safe integer",
        );
      if (spendMinor > record.policy.spend.maxMinorPerEffect)
        throw new EffectAdapterInstallationError(
          "Effect exceeds the installation spend ceiling",
        );
      if (spendMinor > 0) {
        const { currency, mandateId } = record.policy.spend;
        if (!currency || !mandateId)
          throw new EffectAdapterInstallationError(
            "Effect spend requires a configured mandate",
          );
        if (
          !(await options.verifyMandate({
            adapterId: record.adapterId,
            amountMinor: spendMinor,
            currency,
            ...(input.destination ? { destination: input.destination } : {}),
            effect: input.effect,
            mandateId,
            tenantId: record.tenantId,
          }))
        )
          throw new EffectAdapterInstallationError(
            "Effect spend mandate is unavailable",
          );
      }
      const credentials = record.policy.credentials.filter(
        ({ destination }) =>
          !input.destination || destination === input.destination,
      );
      return { adapter, credentials, installation: record };
    },
    disable: async (tenantId, installationId) => {
      const record = await requireRecord(tenantId, installationId);
      await options.store.save({ ...record, enabled: false, updatedAt: now() });
    },
    enable: async (tenantId, installationId) => {
      const {
        ready: _ready,
        reasons: _reasons,
        ...record
      } = await requireReady(tenantId, installationId);
      await Promise.all(
        record.policy.effects.map((effect) =>
          options.adapters.authorize(record.adapterId, effect),
        ),
      );
      await options.store.save({ ...record, enabled: true, updatedAt: now() });
    },
    inventory: async (input) =>
      Promise.all((await options.store.list(input)).map(evaluate)),
    put: async (input) => {
      validateIdentity(input);
      const adapter = await adapterPosture(input.adapterId);
      if (!adapter)
        throw new EffectAdapterInstallationError("Effect adapter not found");
      validatePolicy(adapter.descriptor, input.policy);
      const credentialChecks = await Promise.all(
        input.policy.credentials.map(({ secretAlias }) =>
          options.verifyCredentialAlias(input.tenantId, secretAlias),
        ),
      );
      if (credentialChecks.some((available) => !available))
        throw new EffectAdapterInstallationError(
          "Installation credential alias is unavailable",
        );
      const { currency, mandateId, maxMinorPerEffect } = input.policy.spend;
      if (
        maxMinorPerEffect > 0 &&
        currency &&
        mandateId &&
        !(await options.verifyMandate({
          adapterId: input.adapterId,
          amountMinor: maxMinorPerEffect,
          currency,
          ...(input.policy.destinations[0]
            ? { destination: input.policy.destinations[0] }
            : {}),
          effect: input.policy.effects[0]!,
          mandateId,
          tenantId: input.tenantId,
        }))
      )
        throw new EffectAdapterInstallationError(
          "Installation mandate is unavailable",
        );
      const existing = await options.store.get(
        input.tenantId,
        input.installationId,
      );
      if (
        existing &&
        (existing.adapterId !== input.adapterId ||
          existing.tenantId !== input.tenantId)
      )
        throw new EffectAdapterInstallationError(
          "Installation identity cannot be reassigned",
        );
      const timestamp = now();
      const record: EffectAdapterInstallationRecord = {
        adapterId: input.adapterId,
        adapterVersion: adapter.descriptor.version,
        descriptorDigest: adapter.descriptorDigest,
        enabled: false,
        installationId: input.installationId,
        installedAt: existing?.installedAt ?? timestamp,
        policy: input.policy,
        tenantId: input.tenantId,
        updatedAt: timestamp,
      };
      await options.store.save(record);
      return record;
    },
  };
};
