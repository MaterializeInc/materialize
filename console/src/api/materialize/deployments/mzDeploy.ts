// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/*
 * Reads of mz-deploy's `_mz_deploy` tracking database.
 *
 * These are user objects, which `mz_catalog_server` refuses to query, so every
 * request here runs on `_mz_deploy_server`. That is the cluster mz-deploy
 * itself queries through, and `setup` indexes the views read below on it.
 */

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { executeSqlV2, queryBuilder } from "~/api/materialize";
import DatabaseError from "~/api/materialize/DatabaseError";
import {
  DeploymentRow,
  DeployObjectRow,
} from "~/platform/deployments/deploymentModel";

export const MZ_DEPLOY_CLUSTER = "_mz_deploy_server";

const sessionVariables = { cluster: MZ_DEPLOY_CLUSTER };

/**
 * mz-deploy has not been set up in this environment, or the user cannot see
 * its tracking database.
 */
export function isMzDeployMissing(error: unknown) {
  return (
    error instanceof DatabaseError &&
    /unknown (database|cluster) '_mz_deploy/.test(error.message)
  );
}

export interface DeploymentClusterRow {
  deployId: string;
  /** The production cluster the deploy stages a copy of. */
  name: string;
}

export function buildDeploymentsQueries() {
  return [
    sql<DeploymentRow>`
      SELECT
        deploy_id AS "deployId",
        deployed_at AS "deployedAt",
        promoted_at AS "promotedAt",
        database,
        schema,
        deployed_by AS "deployedBy",
        commit,
        kind,
        mode
      FROM _mz_deploy.public.deployments`.compile(queryBuilder),
    // The view joins `mz_clusters` by ID, so a promoted deploy's clusters drop
    // out once the swap retires the old production cluster.
    sql<DeploymentClusterRow>`
      SELECT deploy_id AS "deployId", name
      FROM _mz_deploy.public.deployment_clusters`.compile(queryBuilder),
  ] as const;
}

export async function fetchDeployments({
  queryKey,
  requestOptions,
}: {
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const [deployments, clusters] = await executeSqlV2({
    queries: buildDeploymentsQueries(),
    queryKey,
    requestOptions,
    sessionVariables,
  });
  return { deployments: deployments.rows, clusters: clusters.rows };
}

export interface PendingStatementRow {
  sequenceNum: number;
  database: string;
  schema: string;
  object: string;
  statementSql: string;
  statementKind: string;
}

export function buildDeploymentDetailQueries(deployIds: string[]) {
  return [
    sql<DeployObjectRow>`
      SELECT deploy_id AS "deployId", database, schema, object, hash
      FROM _mz_deploy.public.objects
      WHERE deploy_id IN (${sql.join(deployIds)})`.compile(queryBuilder),
    sql<PendingStatementRow>`
      SELECT
        sequence_num AS "sequenceNum",
        database,
        schema,
        object,
        statement_sql AS "statementSql",
        statement_kind AS "statementKind"
      FROM _mz_deploy.public.pending_statements
      WHERE deploy_id = ${deployIds[0]} AND executed_at IS NULL
      ORDER BY sequence_num`.compile(queryBuilder),
  ] as const;
}

/**
 * Object hashes for a deploy and its baselines, plus the statements `promote`
 * will run. The first ID is the deploy being inspected.
 */
export async function fetchDeploymentDetail({
  deployIds,
  queryKey,
  requestOptions,
}: {
  deployIds: string[];
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const [objects, pending] = await executeSqlV2({
    queries: buildDeploymentDetailQueries(deployIds),
    queryKey,
    requestOptions,
    sessionVariables,
  });
  return { objects: objects.rows, pendingStatements: pending.rows };
}
