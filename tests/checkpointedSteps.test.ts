import { expect, test } from "bun:test";
import {
  runCheckpointedSteps,
  UnknownEffectOutcomeError,
  type VersionedStepCheckpoint,
  type StepCheckpoint,
} from "../src";

const fixture = () => {
  let saved: VersionedStepCheckpoint<string> | null = null;
  const calls: number[] = [];
  return {
    calls,
    options: {
      planKey: "plan",
      steps: ["one", "two", "three"],
      signal: new AbortController().signal,
      load: async () => structuredClone(saved),
      save: async (revision: number, value: StepCheckpoint<string>) => {
        if (revision !== (saved?.revision ?? 0))
          throw new Error("CAS mismatch");
        saved = { revision: revision + 1, value: structuredClone(value) };
        return saved.revision;
      },
      beforeStep: async () => true,
      runStep: async (step: string, index: number) => {
        calls.push(index);
        return step;
      },
    },
  };
};

test("reconnecting returns completed output without repeating provider calls", async () => {
  const { options, calls } = fixture();
  const result = await runCheckpointedSteps(options);
  expect(await runCheckpointedSteps(options)).toEqual(result);
  expect(calls).toEqual([0, 1, 2]);
});
test("recovery between steps continues only unfinished work", async () => {
  const { options, calls } = fixture();
  await expect(
    runCheckpointedSteps({
      ...options,
      beforeStep: async (index) => {
        if (index === 1) throw new Error("worker stopped before provider");
        return true;
      },
    }),
  ).rejects.toThrow("worker stopped");
  expect((await runCheckpointedSteps(options)).completed).toEqual([
    "one",
    "two",
    "three",
  ]);
  expect(calls).toEqual([0, 1, 2]);
});
test("uncertain provider or usage persistence never retries the marked step", async () => {
  const { options } = fixture();
  let calls = 0;
  const failed = {
    ...options,
    runStep: async () => {
      calls++;
      throw new Error("usage write lost");
    },
  };
  await expect(runCheckpointedSteps(failed)).rejects.toBeInstanceOf(
    UnknownEffectOutcomeError,
  );
  await expect(runCheckpointedSteps(failed)).rejects.toBeInstanceOf(
    UnknownEffectOutcomeError,
  );
  expect(calls).toBe(1);
});
test("post-provider checkpoint failure retains uncertainty and prior results", async () => {
  const { options, calls } = fixture();
  await expect(
    runCheckpointedSteps({
      ...options,
      save: async (revision, value) => {
        if (value.completed.length === 2) throw new Error("database lost");
        return options.save(revision, value);
      },
    }),
  ).rejects.toBeInstanceOf(UnknownEffectOutcomeError);
  expect((await options.load())?.value.completed).toEqual(["one"]);
  await expect(runCheckpointedSteps(options)).rejects.toBeInstanceOf(
    UnknownEffectOutcomeError,
  );
  expect(calls).toEqual([0, 1]);
});
test("budget stop is durable and cannot gain more steps on reconnection", async () => {
  const { options, calls } = fixture();
  const result = await runCheckpointedSteps({
    ...options,
    beforeStep: async (index) => index < 1,
  });
  expect(result.stopped).toBe(true);
  expect(await runCheckpointedSteps(options)).toEqual(result);
  expect(calls).toEqual([0]);
  await expect(
    runCheckpointedSteps({ ...options, planKey: "changed" }),
  ).rejects.toBeInstanceOf(UnknownEffectOutcomeError);
});
