import {
  effectAdapterInstallationsPostgresSchemaSql,
  effectAdapterPurposeBoundInstallationsPostgresSchemaSql,
} from "./adapterInstallations";
import { effectAdapterRegistryPostgresSchemaSql } from "./adapterRegistry";
import { effectEvidencePostgresSchemaSql } from "./evidence";
import {
  effectRecoveryPostgresSchemaSql,
  executionPostgresSchemaSql,
  executionTenantInventoryPostgresSchemaSql,
} from "./postgres";
import { effectAdapterReconciliationPostgresSchemaSql } from "./reconciliation";
import { effectReconciliationSchedulerPostgresSchemaSql } from "./scheduler";

export type ExecutionPostgresMigration = {
  id: string;
  packageName: "@absolutejs/execution";
  packageVersion: string;
  sql: string;
};

/**
 * The complete, ordered PostgreSQL migration contract for Execution.
 *
 * Hosts must consume this manifest rather than selecting individual schema
 * helpers. A helper can remain split for focused package use, while the
 * manifest guarantees that an application upgrading the package cannot omit
 * an earlier additive upgrade such as tenant inventory.
 */
export const executionPostgresMigrations = (
  namespace = "execution",
): ExecutionPostgresMigration[] => [
  {
    id: "execution@0.2.0",
    packageName: "@absolutejs/execution",
    packageVersion: "0.2.0",
    sql: executionPostgresSchemaSql(namespace),
  },
  {
    id: "execution-tenant-inventory@0.3.0",
    packageName: "@absolutejs/execution",
    packageVersion: "0.3.0",
    sql: executionTenantInventoryPostgresSchemaSql(namespace),
  },
  {
    id: "execution-adapter-registry@0.4.0",
    packageName: "@absolutejs/execution",
    packageVersion: "0.4.0",
    sql: effectAdapterRegistryPostgresSchemaSql(namespace),
  },
  {
    id: "execution-adapter-installations@0.5.0",
    packageName: "@absolutejs/execution",
    packageVersion: "0.5.0",
    sql: effectAdapterInstallationsPostgresSchemaSql(namespace),
  },
  {
    id: "execution-effect-recovery@0.7.0",
    packageName: "@absolutejs/execution",
    packageVersion: "0.7.0",
    sql: effectRecoveryPostgresSchemaSql(namespace),
  },
  {
    id: "execution-effect-evidence@0.8.0",
    packageName: "@absolutejs/execution",
    packageVersion: "0.8.0",
    sql: effectEvidencePostgresSchemaSql(namespace),
  },
  {
    id: "execution-effect-reconciliation@0.10.0",
    packageName: "@absolutejs/execution",
    packageVersion: "0.10.0",
    sql: effectAdapterReconciliationPostgresSchemaSql(namespace),
  },
  {
    id: "execution-reconciliation-scheduler@0.10.2",
    packageName: "@absolutejs/execution",
    packageVersion: "0.10.2",
    sql: effectReconciliationSchedulerPostgresSchemaSql(namespace),
  },
  {
    id: "execution-purpose-bound-installations@0.14.1",
    packageName: "@absolutejs/execution",
    packageVersion: "0.14.1",
    sql: effectAdapterPurposeBoundInstallationsPostgresSchemaSql(namespace),
  },
];
