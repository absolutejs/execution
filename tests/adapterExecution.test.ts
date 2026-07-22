import { describe, expect, test } from "bun:test";
import {
  createEffectAdapterExecutionHandler,
  effectAdapterExecutionInputDigest,
  EffectAdapterExecutionError,
  UnknownEffectOutcomeError,
  type EffectAdapterDescriptor,
  type EffectAdapterDriver,
  type EffectAdapterInstallationRegistry,
  type EffectHandlerContext,
} from "../src";

const DESTINATION = "https://api.example.test";
const descriptor: EffectAdapterDescriptor = {
  adapterId: "provider",
  compensation: { supported: false },
  credentialBindings: [
    {
      alias: "API_TOKEN",
      destination: DESTINATION,
      mode: "provider-sdk",
    },
  ],
  destinations: [{ kind: "https-origin", value: DESTINATION }],
  effects: ["message.send"],
  idempotency: { scope: "tenant-effect", supported: true },
  reconciliation: { mode: "manual" },
  spendAuthority: { canSpend: false, currencies: [], requiresMandate: false },
  title: "Provider",
  version: "1.0.0",
};

const authorization = {
  adapter: descriptor,
  credentials: [
    {
      adapterAlias: "API_TOKEN",
      destination: DESTINATION,
      secretAlias: "PROJECT_API_TOKEN",
    },
  ],
  installation: {
    adapterId: descriptor.adapterId,
    adapterVersion: descriptor.version,
    descriptorDigest: "sha256:descriptor",
    enabled: true,
    installationId: "installation-a",
    installedAt: 1,
    policy: {
      credentials: [],
      destinations: [DESTINATION],
      effects: ["message.send"],
      spend: { currency: null, mandateId: null, maxMinorPerEffect: 0 },
    },
    tenantId: "tenant-a",
    updatedAt: 1,
  },
};

const context = (inputDigest: string): EffectHandlerContext => ({
  actionId: "action-a",
  effectId: "effect-a",
  idempotencyKey: "stable-key",
  inputDigest,
  signal: new AbortController().signal,
  tenantId: "tenant-a",
});

const input = {
  destination: DESTINATION,
  effect: "message.send",
  installationId: "installation-a",
  payload: { subject: "hello" },
};
const INPUT_DIGEST = await effectAdapterExecutionInputDigest(input);

const installationRegistry = (
  authorize: EffectAdapterInstallationRegistry["authorize"],
) => ({ authorize });

const driver = (
  execute: EffectAdapterDriver["execute"] = async () => ({ sent: true }),
): EffectAdapterDriver => ({
  adapterId: descriptor.adapterId,
  capabilities: {
    compensation: false,
    idempotency: true,
    reconciliation: "manual",
  },
  execute,
  version: descriptor.version,
});

describe("installed effect adapter execution bridge", () => {
  test("authorizes before resolving any credential material", async () => {
    let resolutions = 0;
    const handler = createEffectAdapterExecutionHandler({
      driver: driver(),
      installations: installationRegistry(async () => {
        throw new Error("denied");
      }),
      resolveCredential: async () => {
        resolutions += 1;
        return "secret";
      },
    });

    await expect(handler.execute(input, context(INPUT_DIGEST))).rejects.toThrow(
      "denied",
    );
    expect(resolutions).toBe(0);
  });

  test("resolves only authorized aliases inside the driver context", async () => {
    const resolved: string[] = [];
    const seen: string[] = [];
    const handler = createEffectAdapterExecutionHandler({
      driver: driver(async (_payload, execution) => {
        seen.push(...execution.credentials.map(({ value }) => value));
        return { providerId: "provider-1" };
      }),
      installations: installationRegistry(async () => authorization),
      resolveCredential: async ({ secretAlias }) => {
        resolved.push(secretAlias);
        return "credential-value";
      },
    });

    const result = await handler.execute(input, context(INPUT_DIGEST));
    expect(resolved).toEqual(["PROJECT_API_TOKEN"]);
    expect(seen).toEqual(["credential-value"]);
    expect(JSON.stringify(result)).not.toContain("credential-value");
  });

  test("extracts only a descriptor-bound provider reconciliation reference", async () => {
    const queryDescriptor: EffectAdapterDescriptor = {
      ...descriptor,
      reconciliation: {
        mode: "query",
        query: {
          credentialAlias: "API_TOKEN",
          health: {
            staleAfterMs: 60_000,
            strategy: "last-successful-query",
          },
          pollingIntervalMs: 10_000,
          provider: "provider",
          requiresReference: true,
          rotation: { mode: "replace", verification: "successful-query" },
          supportedOutcomes: ["confirmed_succeeded"],
        },
      },
    };
    const queryAuthorization = { ...authorization, adapter: queryDescriptor };
    const queryDriver: EffectAdapterDriver<
      { subject: string },
      { id: string; privateResponse: string }
    > = {
      adapterId: queryDescriptor.adapterId,
      capabilities: {
        compensation: false,
        idempotency: true,
        reconciliation: "query",
      },
      execute: async () => ({
        id: "provider-resource-1",
        privateResponse: "never-retain-this",
      }),
      reconciliationReference: (output) => ({
        provider: "provider",
        resourceId: output.id,
      }),
      version: queryDescriptor.version,
    };
    const handler = createEffectAdapterExecutionHandler({
      driver: queryDriver,
      installations: installationRegistry(async () => queryAuthorization),
      resolveCredential: async () => "credential-value",
    });

    const result = await handler.execute(input, context(INPUT_DIGEST));
    expect(result).toMatchObject({
      reconciliationReference: {
        adapterId: "provider",
        provider: "provider",
        resourceId: "provider-resource-1",
      },
    });
    expect(
      JSON.stringify(
        (result as { reconciliationReference: unknown })
          .reconciliationReference,
      ),
    ).not.toContain("never-retain-this");
  });

  test("binds exact tenant, effect, destination, and idempotency context", async () => {
    let authorized: unknown;
    let received: unknown;
    const handler = createEffectAdapterExecutionHandler({
      driver: driver(async (_payload, execution) => {
        received = execution;
        return { sent: true };
      }),
      installations: installationRegistry(async (request) => {
        authorized = request;
        return authorization;
      }),
      resolveCredential: async () => "credential-value",
    });

    await handler.execute(input, context(INPUT_DIGEST));
    expect(authorized).toEqual({
      destination: DESTINATION,
      effect: "message.send",
      installationId: "installation-a",
      tenantId: "tenant-a",
    });
    expect(received).toMatchObject({
      destination: DESTINATION,
      effect: "message.send",
      idempotencyKey: "stable-key",
      installationId: "installation-a",
      tenantId: "tenant-a",
    });
  });

  test("rejects runtime capabilities that differ from certification", async () => {
    let invoked = false;
    let resolutions = 0;
    const mismatched = driver(async () => {
      invoked = true;
    });
    mismatched.capabilities.reconciliation = "query";
    const handler = createEffectAdapterExecutionHandler({
      driver: mismatched,
      installations: installationRegistry(async () => authorization),
      resolveCredential: async () => {
        resolutions += 1;
        return "credential-value";
      },
    });

    await expect(
      handler.execute(input, context(INPUT_DIGEST)),
    ).rejects.toBeInstanceOf(EffectAdapterExecutionError);
    expect(invoked).toBe(false);
    expect(resolutions).toBe(0);
  });

  test("rejects an empty idempotency key before credential resolution", async () => {
    let resolutions = 0;
    const handler = createEffectAdapterExecutionHandler({
      driver: driver(),
      installations: installationRegistry(async () => authorization),
      resolveCredential: async () => {
        resolutions += 1;
        return "credential-value";
      },
    });

    await expect(
      handler.execute(input, {
        ...context(INPUT_DIGEST),
        idempotencyKey: "",
      }),
    ).rejects.toThrow("stable effect idempotency key");
    expect(resolutions).toBe(0);
  });

  test("preserves unknown outcomes for queue quarantine", async () => {
    const handler = createEffectAdapterExecutionHandler({
      driver: driver(async () => {
        throw new UnknownEffectOutcomeError("provider outcome unknown");
      }),
      installations: installationRegistry(async () => authorization),
      resolveCredential: async () => "credential-value",
    });

    await expect(
      handler.execute(input, context(INPUT_DIGEST)),
    ).rejects.toBeInstanceOf(UnknownEffectOutcomeError);
  });

  test("settles spending only after provider success with no credential material", async () => {
    const spendingInput = { ...input, spendMinor: 400 };
    const spendingAuthorization = {
      ...authorization,
      installation: {
        ...authorization.installation,
        policy: {
          ...authorization.installation.policy,
          spend: {
            currency: "USD",
            mandateId: "mandate-a",
            maxMinorPerEffect: 500,
          },
        },
      },
    };
    const events: string[] = [];
    const handler = createEffectAdapterExecutionHandler({
      driver: driver(async () => {
        events.push("provider");
        return { providerId: "provider-1" };
      }),
      installations: installationRegistry(async () => spendingAuthorization),
      resolveCredential: async () => "credential-value",
      settle: async (settlement) => {
        events.push("settlement");
        expect(settlement.authorization.spendMinor).toBe(400);
        expect(settlement.authorization.spendBinding).toBe(
          spendingContext.inputDigest,
        );
        expect(settlement.installation.policy.spend.mandateId).toBe(
          "mandate-a",
        );
        expect(JSON.stringify(settlement)).not.toContain("credential-value");
      },
    });
    const spendingContext = context(
      await effectAdapterExecutionInputDigest(spendingInput),
    );
    await handler.execute(spendingInput, spendingContext);
    expect(events).toEqual(["provider", "settlement"]);
  });

  test("refuses an unhandled spending effect after provider success", async () => {
    const spendingInput = { ...input, spendMinor: 400 };
    let providerCalls = 0;
    const handler = createEffectAdapterExecutionHandler({
      driver: driver(async () => {
        providerCalls += 1;
        return { providerId: "provider-1" };
      }),
      installations: installationRegistry(async () => authorization),
      resolveCredential: async () => "credential-value",
    });
    await expect(
      handler.execute(
        spendingInput,
        context(await effectAdapterExecutionInputDigest(spendingInput)),
      ),
    ).rejects.toThrow("requires a settlement handler");
    expect(providerCalls).toBe(1);
  });

  test("refunds settlement only after provider compensation succeeds", async () => {
    const compensatedDescriptor: EffectAdapterDescriptor = {
      ...descriptor,
      compensation: { supported: true },
    };
    const spendingAuthorization = {
      ...authorization,
      adapter: compensatedDescriptor,
      installation: {
        ...authorization.installation,
        policy: {
          ...authorization.installation.policy,
          spend: {
            currency: "USD",
            mandateId: "mandate-a",
            maxMinorPerEffect: 500,
          },
        },
      },
    };
    const events: string[] = [];
    const compensatedDriver: EffectAdapterDriver = {
      ...driver(async () => {
        events.push("provider");
        return { providerId: "provider-1" };
      }),
      capabilities: {
        compensation: true,
        idempotency: true,
        reconciliation: "manual",
      },
      compensate: async () => {
        events.push("provider-compensation");
      },
    };
    const handler = createEffectAdapterExecutionHandler({
      driver: compensatedDriver,
      installations: installationRegistry(async () => spendingAuthorization),
      refundSettlement: async ({ result }) => {
        events.push("settlement-refund");
        expect(result.settlement).toEqual({
          currency: "USD",
          mandateId: "mandate-a",
          spendMinor: 400,
        });
      },
      resolveCredential: async () => "credential-value",
      settle: async () => {
        events.push("settlement");
      },
    });
    const spendingInput = { ...input, spendMinor: 400 };
    const spendingContext = context(
      await effectAdapterExecutionInputDigest(spendingInput),
    );
    const result = await handler.execute(spendingInput, spendingContext);
    if (!handler.compensate) throw new Error("missing compensation handler");
    await handler.compensate(result, spendingContext);
    expect(events).toEqual([
      "provider",
      "settlement",
      "provider-compensation",
      "settlement-refund",
    ]);
  });

  test("rejects mutated input before authorization or credential resolution", async () => {
    let authorizations = 0;
    let resolutions = 0;
    const handler = createEffectAdapterExecutionHandler({
      driver: driver(),
      installations: installationRegistry(async () => {
        authorizations += 1;
        return authorization;
      }),
      resolveCredential: async () => {
        resolutions += 1;
        return "credential-value";
      },
    });

    await expect(
      handler.execute(
        { ...input, payload: { subject: "mutated" } },
        context(INPUT_DIGEST),
      ),
    ).rejects.toThrow("authorized digest");
    expect(authorizations).toBe(0);
    expect(resolutions).toBe(0);
  });
});
