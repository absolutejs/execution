import { UnknownEffectOutcomeError } from "./worker";

export type StepCheckpoint<Result> = {
  planKey: string;
  completed: Result[];
  inFlight: number | null;
  stopped: boolean;
};
export type VersionedStepCheckpoint<Result> = {
  revision: number;
  value: StepCheckpoint<Result>;
};
export type CheckpointedStepsOptions<Step, Result> = {
  planKey: string;
  steps: readonly Step[];
  signal: AbortSignal;
  load: () => Promise<VersionedStepCheckpoint<Result> | null>;
  /** Must atomically compare revision and persist the entire checkpoint. */
  save: (
    expectedRevision: number,
    value: StepCheckpoint<Result>,
  ) => Promise<number>;
  /** Return false to durably stop before spending on another step. */
  beforeStep: (index: number) => Promise<boolean>;
  /** Return only after known usage and the result are durable enough to checkpoint.
   * Disable provider retries unless the provider supports safe idempotency. */
  runStep: (step: Step, index: number, signal: AbortSignal) => Promise<Result>;
};

/** Run under an exclusive, renewed execution lease. Completed steps are reused;
 * an interrupted provider step is quarantined, never automatically repeated. */
export const runCheckpointedSteps = async <Step, Result>(
  options: CheckpointedStepsOptions<Step, Result>,
): Promise<StepCheckpoint<Result>> => {
  if (!options.planKey || options.steps.length === 0)
    throw new Error("A nonempty immutable step plan is required");
  const saved = await options.load();
  let revision = saved?.revision ?? 0;
  let checkpoint: StepCheckpoint<Result> = saved?.value ?? {
    planKey: options.planKey,
    completed: [],
    inFlight: null,
    stopped: false,
  };
  if (
    checkpoint.planKey !== options.planKey ||
    checkpoint.completed.length > options.steps.length
  )
    throw new UnknownEffectOutcomeError(
      "Checkpoint does not match the immutable plan",
    );
  if (checkpoint.inFlight !== null)
    throw new UnknownEffectOutcomeError(
      "A prior step has an uncertain outcome; do not repeat it",
    );
  const persist = async (value: StepCheckpoint<Result>) => {
    revision = await options.save(revision, structuredClone(value));
    checkpoint = value;
  };
  while (
    !checkpoint.stopped &&
    checkpoint.completed.length < options.steps.length
  ) {
    options.signal.throwIfAborted();
    const index = checkpoint.completed.length;
    if (!(await options.beforeStep(index))) {
      await persist({ ...checkpoint, stopped: true });
      break;
    }
    options.signal.throwIfAborted();
    await persist({ ...checkpoint, inFlight: index });
    try {
      // Once the marker is durable, all errors are uncertain. Even an abort can
      // race with a provider accepting a request or with metering persistence.
      options.signal.throwIfAborted();
      const result = await options.runStep(
        options.steps[index]!,
        index,
        options.signal,
      );
      await persist({
        ...checkpoint,
        completed: [...checkpoint.completed, result],
        inFlight: null,
      });
    } catch {
      throw new UnknownEffectOutcomeError(
        "Step result or metering could not be checkpointed; reconciliation required",
      );
    }
  }
  return checkpoint;
};
