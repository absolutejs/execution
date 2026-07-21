import type {
  EffectAdapterCredentialInstallation,
  EffectAdapterInstallationRegistry,
} from "./adapterInstallations";
import type { EffectAdapterDescriptor } from "./adapterRegistry";
import type { EffectHandler, EffectHandlerContext } from "./types";

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
  version: string;
};

export type EffectAdapterExecutionResult<Output = unknown> = {
  adapterId: string;
  destination?: string;
  effect: string;
  installationId: string;
  output: Output;
};

export class EffectAdapterExecutionError extends Error {}

const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const envelope = (value: unknown): EffectAdapterExecutionEnvelope => {
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
    const credentials = await resolveCredentials({
      credentials: authorization.credentials,
      descriptor: authorization.adapter,
      resolveCredential: options.resolveCredential,
      tenantId: context.tenantId,
    });

    return driverContext(context, input, credentials);
  };
  const handler: EffectHandler = {
    execute: async (value, context) => {
      const input = envelope(value) as EffectAdapterExecutionEnvelope<Input>;
      const output = await options.driver.execute(
        input.payload,
        await prepare(input, context),
      );

      return {
        adapterId: options.driver.adapterId,
        ...(input.destination ? { destination: input.destination } : {}),
        effect: input.effect,
        installationId: input.installationId,
        output,
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
        await prepare(input, context),
      );
    };

  return handler;
};
