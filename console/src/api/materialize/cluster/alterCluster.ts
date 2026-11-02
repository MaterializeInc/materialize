// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { sql } from "kysely";

import { escapedLiteral as lit, queryBuilder } from "~/api/materialize";
import { notNullOrUndefined } from "~/util";

import { executeSqlV2 } from "../executeSqlV2";

export interface AlterClusterSettingsParams {
  clusterName: string;
  /**
   * Leave out what isn't changing. While a resize runs, setting the size and
   * replication factor back to the ones `mz_clusters` reports cancels it.
   */
  size?: string;
  replicas?: number;
}

export interface AlterClusterNameParams {
  clusterName: string;
  name: string;
}

export const alterClusterSettingsStatement = (
  values: AlterClusterSettingsParams,
) => {
  const options = [
    values.size === undefined ? undefined : sql`SIZE = ${lit(values.size)}`,
    values.replicas === undefined
      ? undefined
      : sql`REPLICATION FACTOR = ${sql.raw(values.replicas.toString())}`,
  ].filter(notNullOrUndefined);
  return sql`ALTER CLUSTER ${sql.id(values.clusterName)} SET (
    ${sql.join(options, sql`,\n    `)}
  )
`;
};

export const alterClusterNameStatement = (values: AlterClusterNameParams) => {
  return sql`ALTER CLUSTER ${sql.id(values.clusterName)} RENAME TO ${sql.id(values.name)}
`;
};

export function alterCluster({
  nameParams,
  settingsParams,
  queryKey,
  requestOptions,
}: {
  nameParams: AlterClusterNameParams;
  settingsParams: AlterClusterSettingsParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const hasSettings =
    settingsParams.size !== undefined || settingsParams.replicas !== undefined;
  const alterSettings = hasSettings
    ? [alterClusterSettingsStatement(settingsParams).compile(queryBuilder)]
    : [];
  const alterName = alterClusterNameStatement(nameParams).compile(queryBuilder);
  return executeSqlV2({
    queries: [...alterSettings, alterName],
    queryKey: queryKey,
    requestOptions,
  });
}
