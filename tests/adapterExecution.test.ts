import { describe, expect, test } from "bun:test";
import {
  createEffectAdapterExecutionHandler,
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

const context = (): EffectHandlerContext => ({
  actionId: "action-a",
  effectId: "effect-a",
  idempotencyKey: "stable-key",
  inputDigest: "sha256:input",
  signal: new AbortController().signal,
  tenantId: "tenant-a",
});

const input = {
  destination: DESTINATION,
  effect: "message.send",
  installationId: "installation-a",
  payload: { subject: "hello" },
};

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

    await expect(handler.execute(input, context())).rejects.toThrow("denied");
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

    const result = await handler.execute(input, context());
    expect(resolved).toEqual(["PROJECT_API_TOKEN"]);
    expect(seen).toEqual(["credential-value"]);
    expect(JSON.stringify(result)).not.toContain("credential-value");
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

    await handler.execute(input, context());
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

    await expect(handler.execute(input, context())).rejects.toBeInstanceOf(
      EffectAdapterExecutionError,
    );
    expect(invoked).toBe(false);
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

    await expect(handler.execute(input, context())).rejects.toBeInstanceOf(
      UnknownEffectOutcomeError,
    );
  });
});
