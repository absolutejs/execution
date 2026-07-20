import { describe, expect, test } from "bun:test";
import { createAgentCertification } from "@absolutejs/agent-conformance";
import {
  createEffectAdapterRegistry,
  createMemoryEffectAdapterRegistryStore,
  effectAdapterDescriptorDigest,
  EffectAdapterActivationError,
  type EffectAdapterDescriptor,
} from "../src";

const descriptor = (version = "1.0.0"): EffectAdapterDescriptor => ({
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

const certificate = async (
  value: EffectAdapterDescriptor,
  issuedAt = "2026-07-20T00:00:00.000Z",
) => ({
  adapterId: value.adapterId,
  adapterVersion: value.version,
  certificate: await createAgentCertification({
    issuedAt,
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
    ],
    subject: { name: value.adapterId, version: value.version },
  }),
  descriptorDigest: await effectAdapterDescriptorDigest(value),
  evidenceReference: "drill:record-hash",
});

describe("effect adapter certification registry", () => {
  test("blocks activation until exact fresh evidence passes", async () => {
    const value = descriptor();
    let evidenceValid = true;
    const registry = createEffectAdapterRegistry({
      now: () => Date.parse("2026-07-20T01:00:00.000Z"),
      store: createMemoryEffectAdapterRegistryStore(),
      verifyEvidence: async () => evidenceValid,
    });
    await registry.register(value);
    await expect(registry.activate(value.adapterId)).rejects.toBeInstanceOf(
      EffectAdapterActivationError,
    );
    await registry.certify(await certificate(value));
    await registry.activate(value.adapterId);
    expect(
      (await registry.authorize(value.adapterId, "simulation.complete"))
        .adapterId,
    ).toBe(value.adapterId);
    evidenceValid = false;
    await expect(
      registry.authorize(value.adapterId, "simulation.complete"),
    ).rejects.toThrow("evidence_unverified");
  });

  test("descriptor drift revokes activation and certification", async () => {
    const store = createMemoryEffectAdapterRegistryStore();
    const registry = createEffectAdapterRegistry({
      now: () => Date.parse("2026-07-20T01:00:00.000Z"),
      store,
      verifyEvidence: async () => true,
    });
    const first = descriptor();
    await registry.register(first);
    await registry.certify(await certificate(first));
    await registry.activate(first.adapterId);
    await registry.register(descriptor("1.1.0"));
    const [posture] = await registry.inventory();
    expect(posture?.active).toBe(false);
    expect(posture?.certification).toBeUndefined();
    expect(posture?.reasons).toEqual(["certification_missing"]);
  });

  test("rejects undeclared credential destinations and unmandated spend", async () => {
    const registry = createEffectAdapterRegistry({
      store: createMemoryEffectAdapterRegistryStore(),
      verifyEvidence: async () => true,
    });
    await expect(
      registry.register({
        ...descriptor(),
        credentialBindings: [
          {
            alias: "TOKEN",
            destination: "https://api.example",
            mode: "http-header",
          },
        ],
      }),
    ).rejects.toThrow("undeclared destination");
    await expect(
      registry.register({
        ...descriptor(),
        spendAuthority: {
          canSpend: true,
          currencies: ["USD"],
          requiresMandate: false,
        },
      }),
    ).rejects.toThrow("require a mandate");
  });
});
