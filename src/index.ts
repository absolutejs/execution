export type EffectStatus = "pending"|"leased"|"succeeded"|"failed"|"unknown"|"dead_letter"|"compensated";
export type EffectRecord = { effectId:string; actionId:string; handler:string; idempotencyKey:string; input:unknown; inputDigest:string; status:EffectStatus; attempts:number; availableAt:number; leaseOwner?:string; leaseExpiresAt?:number; result?:unknown; error?:string; createdAt:number; updatedAt:number };
export type EffectStore = {
  enqueue:(effect:EffectRecord)=>Promise<boolean>;
  claim:(workerId:string, leaseMs:number, now:number)=>Promise<EffectRecord|undefined>;
  heartbeat:(effectId:string, workerId:string, leaseMs:number, now:number)=>Promise<boolean>;
  succeed:(effectId:string, workerId:string, result:unknown, now:number)=>Promise<boolean>;
  fail:(effectId:string, workerId:string, update:{error:string; status:"pending"|"failed"|"unknown"|"dead_letter"; availableAt?:number}, now:number)=>Promise<boolean>;
  get:(effectId:string)=>Promise<EffectRecord|undefined>;
  reconcile:(effectId:string, update:{status:"pending"|"succeeded"|"dead_letter"; result?:unknown; error?:string}, now:number)=>Promise<boolean>;
};
export type EffectHandler = { execute:(input:unknown, context:{idempotencyKey:string; signal:AbortSignal})=>Promise<unknown>; compensate?:(result:unknown, context:{idempotencyKey:string})=>Promise<void> };

export const createMemoryEffectStore = ():EffectStore => {
  const rows=new Map<string,EffectRecord>(); let tail=Promise.resolve();
  const locked=async<T>(run:()=>T|Promise<T>)=>{const previous=tail;let release=()=>{};tail=new Promise<void>(r=>{release=r});await previous;try{return await run()}finally{release()}};
  return {enqueue:(e)=>locked(()=>{if([...rows.values()].some(r=>r.idempotencyKey===e.idempotencyKey))return false;rows.set(e.effectId,structuredClone(e));return true}),
    claim:(worker,lease,now)=>locked(()=>{const row=[...rows.values()].find(r=>(r.status==="pending"||(r.status==="leased"&&(r.leaseExpiresAt??0)<=now))&&r.availableAt<=now);if(!row)return;const next={...row,status:"leased" as const,leaseOwner:worker,leaseExpiresAt:now+lease,attempts:row.attempts+1,updatedAt:now};rows.set(row.effectId,next);return structuredClone(next)}),
    heartbeat:(id,w,l,n)=>locked(()=>{const r=rows.get(id);if(!r||r.status!=="leased"||r.leaseOwner!==w)return false;rows.set(id,{...r,leaseExpiresAt:n+l,updatedAt:n});return true}),
    succeed:(id,w,result,n)=>locked(()=>{const r=rows.get(id);if(!r||r.status!=="leased"||r.leaseOwner!==w)return false;rows.set(id,{...r,status:"succeeded",result,updatedAt:n});return true}),
    fail:(id,w,u,n)=>locked(()=>{const r=rows.get(id);if(!r||r.status!=="leased"||r.leaseOwner!==w)return false;rows.set(id,{...r,...u,leaseOwner:undefined,leaseExpiresAt:undefined,availableAt:u.availableAt??r.availableAt,updatedAt:n});return true}),
    get:async id=>{const r=rows.get(id);return r?structuredClone(r):undefined},
    reconcile:(id,u,n)=>locked(()=>{const r=rows.get(id);if(!r||r.status!=="unknown")return false;rows.set(id,{...r,...u,updatedAt:n});return true})};
};

export const createEffectWorker=({store,handlers,workerId,leaseMs=30_000,maxAttempts=5,now=Date.now}:{store:EffectStore;handlers:Record<string,EffectHandler>;workerId:string;leaseMs?:number;maxAttempts?:number;now?:()=>number})=>({
  runOnce:async()=>{const effect=await store.claim(workerId,leaseMs,now());if(!effect)return undefined;const handler=handlers[effect.handler];if(!handler){await store.fail(effect.effectId,workerId,{error:"Unknown effect handler",status:"dead_letter"},now());return effect.effectId}const controller=new AbortController();try{const result=await handler.execute(effect.input,{idempotencyKey:effect.idempotencyKey,signal:controller.signal});if(!await store.succeed(effect.effectId,workerId,result,now()))throw new Error("Effect lease lost before completion")}catch(error){const message=error instanceof Error?error.message:"Effect failed";const unknown=error instanceof UnknownEffectOutcomeError;await store.fail(effect.effectId,workerId,unknown?{error:message,status:"unknown"}:effect.attempts>=maxAttempts?{error:message,status:"dead_letter"}:{error:message,status:"pending",availableAt:now()+Math.min(60_000,1000*2**(effect.attempts-1))},now())}return effect.effectId}
});
export class UnknownEffectOutcomeError extends Error { constructor(message="Provider outcome is unknown"){super(message);this.name="UnknownEffectOutcomeError"} }

export const executionPostgresSchemaSql=(namespace="execution")=>{if(!/^[a-z_][a-z0-9_]*$/.test(namespace))throw new Error("Execution namespace must be a simple identifier");return `CREATE SCHEMA IF NOT EXISTS ${namespace}; CREATE TABLE IF NOT EXISTS ${namespace}.effects (effect_id text PRIMARY KEY, action_id text NOT NULL, handler text NOT NULL, idempotency_key text NOT NULL UNIQUE, status text NOT NULL, attempts integer NOT NULL DEFAULT 0, available_at bigint NOT NULL, lease_owner text, lease_expires_at bigint, input_digest text NOT NULL, data jsonb NOT NULL, created_at bigint NOT NULL, updated_at bigint NOT NULL); CREATE INDEX IF NOT EXISTS effects_claim_idx ON ${namespace}.effects (status, available_at, lease_expires_at);`;};
