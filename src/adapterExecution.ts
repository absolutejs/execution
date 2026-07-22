import type {
  EffectAdapterCredentialInstallation,
  EffectAdapterInstallationAuthorization,
  EffectAdapterInstallationRegistry,
  EffectAdapterInstallationRecord,
} from "./adapterInstallations";
import {
  effectAdapterQueryReconciliation,
  type EffectAdapterDescriptor,
} from "./adapterRegistry";
import type {
  EffectHandler,
  EffectHandlerContext,
  EffectProviderReconciliationReference,
} from "./types";

export type EffectAdapterExecutionEnvelope<Input = unknown> = {
  destination?: string;
  effect: string;
  installationId: string;
  payload: Input;
  spendMinor?: number;
};

export type EffectAdapterDriverCapabilities = {
  compensation: boolean;
  idempotency: boolean;
  reconciliation: EffectAdapterDescriptor["reconciliation"]["mode"];
};

export type ResolvedEffectAdapterCredential =
  EffectAdapterCredentialInstallation & {
    mode: EffectAdapterDescriptor["credentialBindings"][number]["mode"];
    value: string;
  };

export type EffectAdapterDriverContext = EffectHandlerContext & {
  credentials: ReadonlyArray<ResolvedEffectAdapterCredential>;
  destination?: string;
  effect: string;
  installationId: string;
};

export type EffectAdapterDriver<Input = unknown, Output = unknown> = {
  adapterId: string;
  capabilities: EffectAdapterDriverCapabilities;
  compensate?: (
    output: Output,
    context: EffectAdapterDriverContext,
  ) => Promise<void>;
  execute: (
    input: Input,
    context: EffectAdapterDriverContext,
  ) => Promise<Output>;
  reconciliationReference?: (
    output: Output,
    context: EffectAdapterDriverContext,
  ) => Omit<EffectProviderReconciliationReference, "adapterId"> | undefined;
  version: string;
};

export type EffectAdapterExecutionResult<Output = unknown> = {
  adapterId: string;
  destination?: string;
  effect: string;
  installationId: string;
  output: Output;
  reconciliationReference?: EffectProviderReconciliationReference;
  settlement?: EffectAdapterSettlement;
};

export type EffectAdapterSettlement = {
  currency: string;
  mandateId: string;
  spendMinor: number;
};

export type EffectAdapterSettlementInput<Output = unknown> = {
  authorization: EffectAdapterInstallationAuthorization;
  context: EffectHandlerContext;
  installation: EffectAdapterInstallationRecord;
  output: Output;
  providerReference?: EffectProviderReconciliationReference;
  settlement: EffectAdapterSettlement;
};

export type EffectAdapterSettlementRefundInput<Output = unknown> = {
  context: EffectHandlerContext;
  result: EffectAdapterExecutionResult<Output> & {
    settlement: EffectAdapterSettlement;
  };
};

export class EffectAdapterExecutionError extends Error {}

const normalize = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(normalize);
  if (value !== null && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, entry]) => entry !== undefined)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, entry]) => [key, normalize(entry)]),
    );

  return value;
};

export const effectAdapterExecutionInputDigest = async (value: unknown) => {
  const encoded = new TextEncoder().encode(JSON.stringify(normalize(value)));
  const result = await crypto.subtle.digest("SHA-256", encoded);

  return [...new Uint8Array(result)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
};

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const REFERENCE_VALUE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/;
const PROVIDER_PATTERN = /^[a-z][a-z0-9_-]{0,63}$/;

export const parseEffectProviderReconciliationReference = (
  value: unknown,
): EffectProviderReconciliationReference | undefined => {
  if (value === undefined) return undefined;
  if (
    !record(value) ||
    typeof value.adapterId !== "string" ||
    !value.adapterId.trim() ||
    typeof value.provider !== "string" ||
    !PROVIDER_PATTERN.test(value.provider) ||
    typeof value.resourceId !== "string" ||
    !REFERENCE_VALUE_PATTERN.test(value.resourceId)
  )
    throw new EffectAdapterExecutionError(
      "Provider reconciliation reference is invalid",
    );

  return {
    adapterId: value.adapterId,
    provider: value.provider,
    resourceId: value.resourceId,
  };
};

export const effectProviderReconciliationReferenceFromResult = (
  value: unknown,
) =>
  record(value)
    ? parseEffectProviderReconciliationReference(value.reconciliationReference)
    : undefined;

export const parseEffectAdapterExecutionEnvelope = (
  value: unknown,
): EffectAdapterExecutionEnvelope => {
  if (
    !record(value) ||
    typeof value.installationId !== "string" ||
    !value.installationId.trim() ||
    typeof value.effect !== "string" ||
    !value.effect.trim() ||
    !("payload" in value) ||
    (value.destination !== undefined &&
      (typeof value.destination !== "string" || !value.destination.trim())) ||
    (value.spendMinor !== undefined &&
      (typeof value.spendMinor !== "number" ||
        !Number.isSafeInteger(value.spendMinor) ||
        value.spendMinor < 0))
  )
    throw new EffectAdapterExecutionError(
      "Installed adapter input envelope is invalid",
    );

  return value as EffectAdapterExecutionEnvelope;
};

const resultEnvelope = (value: unknown): EffectAdapterExecutionResult => {
  if (
    !record(value) ||
    typeof value.adapterId !== "string" ||
    typeof value.installationId !== "string" ||
    typeof value.effect !== "string" ||
    !("output" in value) ||
    (value.destination !== undefined && typeof value.destination !== "string")
  )
    throw new EffectAdapterExecutionError(
      "Installed adapter result envelope is invalid",
    );
  if (
    value.settlement !== undefined &&
    (!record(value.settlement) ||
      typeof value.settlement.currency !== "string" ||
      !value.settlement.currency.trim() ||
      typeof value.settlement.mandateId !== "string" ||
      !value.settlement.mandateId.trim() ||
      typeof value.settlement.spendMinor !== "number" ||
      !Number.isSafeInteger(value.settlement.spendMinor) ||
      value.settlement.spendMinor < 1)
  )
    throw new EffectAdapterExecutionError(
      "Installed adapter settlement result is invalid",
    );

  return value as EffectAdapterExecutionResult;
};

const assertDriverMatchesDescriptor = (
  driver: {
    adapterId: string;
    capabilities: EffectAdapterDriverCapabilities;
    compensate?: unknown;
    version: string;
  },
  descriptor: EffectAdapterDescriptor,
) => {
  if (driver.adapterId !== descriptor.adapterId)
    throw new EffectAdapterExecutionError(
      "Runtime adapter identity differs from the authorized descriptor",
    );
  if (driver.version !== descriptor.version)
    throw new EffectAdapterExecutionError(
      "Runtime adapter version differs from the authorized descriptor",
    );
  if (
    driver.capabilities.compensation !== descriptor.compensation.supported ||
    driver.capabilities.idempotency !== descriptor.idempotency.supported ||
    driver.capabilities.reconciliation !== descriptor.reconciliation.mode ||
    driver.capabilities.compensation !== Boolean(driver.compensate)
  )
    throw new EffectAdapterExecutionError(
      "Runtime adapter capabilities differ from the authorized descriptor",
    );
};

const binding = (
  descriptor: EffectAdapterDescriptor,
  credential: EffectAdapterCredentialInstallation,
) =>
  descriptor.credentialBindings.find(
    ({ alias, destination }) =>
      alias === credential.adapterAlias &&
      destination === credential.destination,
  );

const resolveCredentials = async (input: {
  credentials: ReadonlyArray<EffectAdapterCredentialInstallation>;
  descriptor: EffectAdapterDescriptor;
  resolveCredential: (input: {
    adapterAlias: string;
    destination: string;
    secretAlias: string;
    tenantId: string;
  }) => Promise<string | null | undefined>;
  tenantId: string;
}) =>
  Promise.all(
    input.credentials.map(async (credential) => {
      const declared = binding(input.descriptor, credential);
      if (!declared)
        throw new EffectAdapterExecutionError(
          "Authorized credential is absent from the adapter descriptor",
        );
      const value = await input.resolveCredential({
        adapterAlias: credential.adapterAlias,
        destination: credential.destination,
        secretAlias: credential.secretAlias,
        tenantId: input.tenantId,
      });
      if (!value)
        throw new EffectAdapterExecutionError(
          `Credential alias is unavailable: ${credential.secretAlias}`,
        );

      return { ...credential, mode: declared.mode, value };
    }),
  );

const driverContext = (
  context: EffectHandlerContext,
  input: EffectAdapterExecutionEnvelope,
  credentials: ReadonlyArray<ResolvedEffectAdapterCredential>,
): EffectAdapterDriverContext => ({
  ...context,
  credentials,
  ...(input.destination ? { destination: input.destination } : {}),
  effect: input.effect,
  installationId: input.installationId,
});

export const createEffectAdapterExecutionHandler = <Input, Output>(options: {
  driver: EffectAdapterDriver<Input, Output>;
  installations: Pick<EffectAdapterInstallationRegistry, "authorize">;
  resolveCredential: (input: {
    adapterAlias: string;
    destination: string;
    secretAlias: string;
    tenantId: string;
  }) => Promise<string | null | undefined>;
  refundSettlement?: (
    input: EffectAdapterSettlementRefundInput<Output>,
  ) => Promise<void>;
  settle?: (input: EffectAdapterSettlementInput<Output>) => Promise<void>;
}): EffectHandler => {
  const prepare = async (
    input: EffectAdapterExecutionEnvelope,
    context: EffectHandlerContext,
  ) => {
    const authorization = await options.installations.authorize({
      ...(input.destination ? { destination: input.destination } : {}),
      effect: input.effect,
      installationId: input.installationId,
      ...(input.spendMinor === undefined
        ? {}
        : { spendMinor: input.spendMinor }),
      tenantId: context.tenantId,
    });
    assertDriverMatchesDescriptor(options.driver, authorization.adapter);
    if (
      authorization.adapter.idempotency.supported &&
      !context.idempotencyKey.trim()
    )
      throw new EffectAdapterExecutionError(
        "Idempotent adapters require a stable effect idempotency key",
      );
    const credentials = await resolveCredentials({
      credentials: authorization.credentials,
      descriptor: authorization.adapter,
      resolveCredential: options.resolveCredential,
      tenantId: context.tenantId,
    });

    return {
      authorization,
      executionContext: driverContext(context, input, credentials),
    };
  };
  const handler: EffectHandler = {
    execute: async (value, context) => {
      if (
        (await effectAdapterExecutionInputDigest(value)) !== context.inputDigest
      )
        throw new EffectAdapterExecutionError(
          "Installed adapter input differs from its authorized digest",
        );
      const input = parseEffectAdapterExecutionEnvelope(
        value,
      ) as EffectAdapterExecutionEnvelope<Input>;
      const { authorization, executionContext } = await prepare(input, context);
      const output = await options.driver.execute(
        input.payload,
        executionContext,
      );
      const reference = options.driver.reconciliationReference?.(
        output,
        executionContext,
      );
      const reconciliationReference = reference
        ? parseEffectProviderReconciliationReference({
            ...reference,
            adapterId: options.driver.adapterId,
          })
        : undefined;
      const query = effectAdapterQueryReconciliation(
        authorization.adapter.reconciliation,
      );
      if (
        reconciliationReference &&
        (!query || reconciliationReference.provider !== query.provider)
      )
        throw new EffectAdapterExecutionError(
          "Provider reconciliation reference is outside the descriptor contract",
        );
      let settlement: EffectAdapterSettlement | undefined;
      if (input.spendMinor !== undefined && input.spendMinor > 0) {
        if (!options.settle)
          throw new EffectAdapterExecutionError(
            "A spending effect requires a settlement handler",
          );
        const mandateId = authorization.installation.policy.spend.mandateId;
        const currency = authorization.installation.policy.spend.currency;
        if (!mandateId || !currency)
          throw new EffectAdapterExecutionError(
            "A spending effect requires an installed mandate and currency",
          );
        settlement = { currency, mandateId, spendMinor: input.spendMinor };
        await options.settle({
          authorization: {
            ...(input.destination ? { destination: input.destination } : {}),
            effect: input.effect,
            installationId: input.installationId,
            spendMinor: input.spendMinor,
            tenantId: context.tenantId,
          },
          context,
          installation: authorization.installation,
          output,
          ...(reconciliationReference
            ? { providerReference: reconciliationReference }
            : {}),
          settlement,
        });
      }

      return {
        adapterId: options.driver.adapterId,
        ...(input.destination ? { destination: input.destination } : {}),
        effect: input.effect,
        installationId: input.installationId,
        output,
        ...(reconciliationReference ? { reconciliationReference } : {}),
        ...(settlement ? { settlement } : {}),
      } satisfies EffectAdapterExecutionResult<Output>;
    },
  };
  if (options.driver.compensate)
    handler.compensate = async (value, context) => {
      const result = resultEnvelope(
        value,
      ) as EffectAdapterExecutionResult<Output>;
      if (result.adapterId !== options.driver.adapterId)
        throw new EffectAdapterExecutionError(
          "Compensation result belongs to another adapter",
        );
      const input: EffectAdapterExecutionEnvelope = {
        ...(result.destination ? { destination: result.destination } : {}),
        effect: result.effect,
        installationId: result.installationId,
        payload: undefined,
      };
      await options.driver.compensate!(
        result.output,
        (await prepare(input, context)).executionContext,
      );
      if (result.settlement) {
        if (!options.refundSettlement)
          throw new EffectAdapterExecutionError(
            "Compensated spending requires a settlement refund handler",
          );
        await options.refundSettlement({
          context,
          result: {
            ...result,
            settlement: result.settlement,
          },
        });
      }
    };

  return handler;
};
