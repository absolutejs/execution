export type EffectStatus =
  | "pending"
  | "leased"
  | "succeeded"
  | "failed"
  | "unknown"
  | "dead_letter"
  | "compensating"
  | "compensation_failed"
  | "compensated";

export type EffectRecord = {
  actionId: string;
  attempts: number;
  availableAt: number;
  createdAt: number;
  effectId: string;
  error?: string;
  handler: string;
  idempotencyKey: string;
  input: unknown;
  inputDigest: string;
  leaseExpiresAt?: number;
  leaseOwner?: string;
  result?: unknown;
  status: EffectStatus;
  tenantId: string;
  updatedAt: number;
  runId?: string;
};

export type EffectAttemptKind = "execute" | "compensate";
export type EffectAttemptOutcome =
  | "running"
  | "succeeded"
  | "failed"
  | "unknown";

export type EffectAttempt = {
  attemptId: string;
  effectId: string;
  error?: string;
  finishedAt?: number;
  kind: EffectAttemptKind;
  number: number;
  outcome: EffectAttemptOutcome;
  startedAt: number;
  workerId: string;
};

export type EffectOutboxRecord = {
  attempts: number;
  effectId: string;
  eventId: string;
  leaseExpiresAt?: number;
  leaseOwner?: string;
};

export type EffectStore = {
  enqueue: (effect: EffectRecord) => Promise<boolean>;
  claim: (
    workerId: string,
    leaseMs: number,
    now: number,
  ) => Promise<EffectRecord | undefined>;
  claimEffect: (
    effectId: string,
    workerId: string,
    leaseMs: number,
    now: number,
  ) => Promise<EffectRecord | undefined>;
  heartbeat: (
    effectId: string,
    workerId: string,
    leaseMs: number,
    now: number,
  ) => Promise<boolean>;
  succeed: (
    effectId: string,
    workerId: string,
    result: unknown,
    now: number,
  ) => Promise<boolean>;
  fail: (
    effectId: string,
    workerId: string,
    update: {
      availableAt?: number;
      error: string;
      status: "pending" | "failed" | "unknown" | "dead_letter";
    },
    now: number,
  ) => Promise<boolean>;
  get: (effectId: string) => Promise<EffectRecord | undefined>;
  getByIdempotencyKey: (
    tenantId: string,
    idempotencyKey: string,
  ) => Promise<EffectRecord | undefined>;
  list: (input: {
    limit: number;
    runId?: string;
    status?: EffectStatus;
    tenantId?: string;
  }) => Promise<EffectRecord[]>;
  listAttempts: (effectId: string) => Promise<EffectAttempt[]>;
  reconcile: (
    effectId: string,
    update: {
      error?: string;
      result?: unknown;
      status: "pending" | "succeeded" | "dead_letter";
    },
    now: number,
  ) => Promise<boolean>;
  claimOutbox: (
    workerId: string,
    leaseMs: number,
    now: number,
  ) => Promise<EffectOutboxRecord | undefined>;
  publishOutbox: (eventId: string, workerId: string) => Promise<boolean>;
  retryOutbox: (eventId: string, workerId: string) => Promise<boolean>;
  recordAttempt: (attempt: EffectAttempt) => Promise<void>;
  finishAttempt: (
    attemptId: string,
    outcome: Exclude<EffectAttemptOutcome, "running">,
    now: number,
    error?: string,
  ) => Promise<void>;
  startCompensation: (
    effectId: string,
    workerId: string,
    now: number,
  ) => Promise<EffectRecord | undefined>;
  finishCompensation: (
    effectId: string,
    workerId: string,
    now: number,
    error?: string,
  ) => Promise<boolean>;
};

export type EffectHandler = {
  compensate?: (
    result: unknown,
    context: EffectHandlerContext,
  ) => Promise<void>;
  execute: (input: unknown, context: EffectHandlerContext) => Promise<unknown>;
};

export type EffectHandlerContext = {
  actionId: string;
  effectId: string;
  idempotencyKey: string;
  inputDigest: string;
  runId?: string;
  signal: AbortSignal;
  tenantId: string;
};

export type ExecutionJobs = {
  "absolutejs.execution.effect": { effectId: string };
};

/** Narrow structural subset of `@absolutejs/queue`'s JobStore. Keeping this
 * structural prevents Execution from pulling the queue's Elysia peer into
 * applications that only use the effect store. */
export type ExecutionQueueStore = {
  enqueue: (input: {
    idempotencyKey?: string;
    kind: "absolutejs.execution.effect";
    maxAttempts?: number;
    payload: ExecutionJobs["absolutejs.execution.effect"];
    runAt?: number;
  }) => Promise<unknown>;
};
export type ExecutionQueueContext = {
  attempts: number;
  id: string;
  kind: "absolutejs.execution.effect";
  maxAttempts: number;
  signal: AbortSignal;
};
export type ExecutionQueueHandler = (
  payload: ExecutionJobs["absolutejs.execution.effect"],
  context: ExecutionQueueContext,
) => Promise<void> | void;
