import { describe, expect, test } from "bun:test";
import { createAgentCertification } from "@absolutejs/agent-conformance";
import {
  createEffectAdapterInstallationRegistry,
  createEffectAdapterRegistry,
  createMemoryEffectAdapterInstallationStore,
  createMemoryEffectAdapterRegistryStore,
  effectAdapterDescriptorDigest,
  EffectAdapterInstallationError,
  type EffectAdapterDescriptor,
} from "../src";

const NOW = Date.parse("2026-07-21T12:00:00.000Z");

const simulationDescriptor = (version = "1.0.0"): EffectAdapterDescriptor => ({
  adapterId: "simulation",
  compensation: { supported: false },
  credentialBindings: [],
  destinations: [],
  effects: ["simulation.complete"],
  idempotency: { scope: "tenant-effect", supported: true },
  reconciliation: { mode: "manual" },
  spendAuthority: { canSpend: false, currencies: [], requiresMandate: false },
  title: "Simulation",
  version,
});

const providerDescriptor = (): EffectAdapterDescriptor => ({
  adapterId: "provider",
  compensation: { supported: false },
  credentialBindings: [
    {
      alias: "API_TOKEN",
      destination: "https://api.example.test",
      mode: "http-header",
    },
  ],
  destinations: [{ kind: "https-origin", value: "https://api.example.test" }],
  effects: ["message.send"],
  idempotency: { scope: "tenant-effect", supported: true },
  reconciliation: {
    mode: "query",
    query: {
      credentialAlias: "API_TOKEN",
      health: {
        staleAfterMs: 15 * 60 * 1_000,
        strategy: "last-successful-query",
      },
      pollingIntervalMs: 60_000,
      provider: "provider",
      requiresReference: false,
      rotation: { mode: "replace", verification: "successful-query" },
      supportedOutcomes: ["delivered", "failed"],
    },
  },
  spendAuthority: {
    canSpend: true,
    currencies: ["USD"],
    requiresMandate: true,
  },
  title: "Provider",
  version: "1.0.0",
});

const certification = async (descriptor: EffectAdapterDescriptor) => ({
  adapterId: descriptor.adapterId,
  adapterVersion: descriptor.version,
  certificate: await createAgentCertification({
    issuedAt: new Date(NOW).toISOString(),
    reports: [
      {
        failed: 0,
        passed: 1,
        results: [{ name: "execution-boundary/replay", passed: true }],
        suite: "agent-durable-execution-boundary",
      },
      {
        failed: 0,
        passed: 1,
        results: [
          { name: "adapter-registry/uncertified-activation", passed: true },
        ],
        suite: "agent-effect-adapter-registry",
      },
      {
        failed: 0,
        passed: 1,
        results: [{ name: "adapter-installations/default-off", passed: true }],
        suite: "agent-effect-adapter-installations",
      },
      {
        failed: 0,
        passed: 1,
        results: [
          {
            name: "adapter-execution/authorization-before-credentials",
            passed: true,
          },
        ],
        suite: "agent-effect-adapter-execution",
      },
      {
        failed: 0,
        passed: 1,
        results: [
          {
            name: "reconciliation-runtime/authorization-before-query-credentials",
            passed: true,
          },
        ],
        suite: "agent-effect-reconciliation-runtime",
      },
    ],
    subject: { name: descriptor.adapterId, version: descriptor.version },
  }),
  descriptorDigest: await effectAdapterDescriptorDigest(descriptor),
  evidenceReference: "drill:installation-test",
});

const activeAdapters = async (descriptor: EffectAdapterDescriptor) => {
  const adapters = createEffectAdapterRegistry({
    now: () => NOW,
    store: createMemoryEffectAdapterRegistryStore(),
    verifyEvidence: async () => true,
  });
  await adapters.register(descriptor);
  await adapters.certify(await certification(descriptor));
  await adapters.activate(descriptor.adapterId);
  return adapters;
};

const simulationPolicy = {
  credentials: [],
  destinations: [],
  effects: ["simulation.complete"],
  spend: { currency: null, mandateId: null, maxMinorPerEffect: 0 },
} as const;

describe("tenant effect adapter installations", () => {
  test("starts disabled and keeps authorization tenant-fenced", async () => {
    const adapters = await activeAdapters(simulationDescriptor());
    const installations = createEffectAdapterInstallationRegistry({
      adapters,
      now: () => NOW,
      store: createMemoryEffectAdapterInstallationStore(),
      verifyCredentialAlias: async () => true,
      verifyMandate: async () => true,
    });
    await installations.put({
      adapterId: "simulation",
      installationId: "installation-a",
      policy: simulationPolicy,
      tenantId: "tenant-a",
    });
    expect(
      (await installations.inventory({ tenantId: "tenant-a" }))[0],
    ).toMatchObject({
      enabled: false,
      ready: true,
    });
    await expect(
      installations.authorize({
        effect: "simulation.complete",
        installationId: "installation-a",
        tenantId: "tenant-a",
      }),
    ).rejects.toThrow("disabled");
    await expect(
      installations.enable("tenant-b", "installation-a"),
    ).rejects.toThrow("not found");
    await expect(
      installations.put({
        adapterId: "simulation",
        installationId: "installation-a",
        policy: simulationPolicy,
        tenantId: "tenant-b",
      }),
    ).rejects.toThrow("belongs to another tenant");
    await installations.enable("tenant-a", "installation-a");
    expect(
      (
        await installations.authorize({
          effect: "simulation.complete",
          installationId: "installation-a",
          tenantId: "tenant-a",
        })
      ).adapter.adapterId,
    ).toBe("simulation");
  });

  test("fails closed when the globally certified descriptor drifts", async () => {
    const adapters = await activeAdapters(simulationDescriptor());
    const installations = createEffectAdapterInstallationRegistry({
      adapters,
      now: () => NOW,
      store: createMemoryEffectAdapterInstallationStore(),
      verifyCredentialAlias: async () => true,
      verifyMandate: async () => true,
    });
    await installations.put({
      adapterId: "simulation",
      installationId: "installation-a",
      policy: simulationPolicy,
      tenantId: "tenant-a",
    });
    await installations.enable("tenant-a", "installation-a");
    await adapters.register(simulationDescriptor("1.1.0"));
    await expect(
      installations.authorize({
        effect: "simulation.complete",
        installationId: "installation-a",
        tenantId: "tenant-a",
      }),
    ).rejects.toThrow("adapter_inactive");
    expect((await installations.inventory())[0]?.reasons).toContain(
      "descriptor_pin_mismatch",
    );
  });

  test("binds exact destinations, secret aliases, mandate, and spend ceiling", async () => {
    const adapters = await activeAdapters(providerDescriptor());
    const installations = createEffectAdapterInstallationRegistry({
      adapters,
      now: () => NOW,
      store: createMemoryEffectAdapterInstallationStore(),
      verifyCredentialAlias: async (tenantId, alias) =>
        tenantId === "tenant-a" && alias === "PROJECT_PROVIDER_TOKEN",
      verifyMandate: async ({
        amountMinor,
        destination,
        effect,
        mandateId,
        spendBinding,
        tenantId,
      }) =>
        tenantId === "tenant-a" &&
        mandateId === "mandate-a" &&
        effect === "message.send" &&
        destination === "https://api.example.test" &&
        (spendBinding === undefined || spendBinding === "digest-a") &&
        amountMinor <= 500,
    });
    const policy = {
      credentials: [
        {
          adapterAlias: "API_TOKEN",
          destination: "https://api.example.test",
          secretAlias: "PROJECT_PROVIDER_TOKEN",
        },
      ],
      destinations: ["https://api.example.test"],
      effects: ["message.send"],
      spend: {
        currency: "USD",
        mandateId: "mandate-a",
        maxMinorPerEffect: 500,
      },
    } as const;
    await installations.put({
      adapterId: "provider",
      installationId: "installation-provider",
      policy,
      tenantId: "tenant-a",
    });
    await installations.enable("tenant-a", "installation-provider");
    await expect(
      installations.authorize({
        destination: "https://other.example.test",
        effect: "message.send",
        installationId: "installation-provider",
        tenantId: "tenant-a",
      }),
    ).rejects.toThrow("outside installation scope");
    await expect(
      installations.authorize({
        currency: "USD",
        destination: "https://api.example.test",
        effect: "message.send",
        installationId: "installation-provider",
        mandateId: "mandate-a",
        spendMinor: 501,
        tenantId: "tenant-a",
      }),
    ).rejects.toThrow("spend ceiling");
    const authorized = await installations.authorize({
      currency: "USD",
      destination: "https://api.example.test",
      effect: "message.send",
      installationId: "installation-provider",
      mandateId: "mandate-a",
      spendBinding: "digest-a",
      spendMinor: 250,
      tenantId: "tenant-a",
    });
    expect(authorized.credentials).toEqual(policy.credentials);
    expect("value" in authorized.credentials[0]!).toBe(false);
    await expect(
      installations.put({
        adapterId: "provider",
        installationId: "installation-bad-secret",
        policy: {
          ...policy,
          credentials: [
            { ...policy.credentials[0], secretAlias: "MISSING_SECRET" },
          ],
        },
        tenantId: "tenant-a",
      }),
    ).rejects.toBeInstanceOf(EffectAdapterInstallationError);
  });
});
