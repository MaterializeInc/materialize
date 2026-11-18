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

import { executeSqlV2, queryBuilder } from "~/api/materialize";
import { hasSuperUserPrivileges } from "~/api/materialize/expressionBuilders";

/** Whether the current role can run queries on a cluster. */
export function buildClusterUsagePrivilegeQuery(clusterId: string) {
  return queryBuilder
    .selectFrom("mz_clusters as c")
    .where("c.id", "=", clusterId)
    .select((eb) =>
      eb
        .or([
          sql<boolean>`(${hasSuperUserPrivileges()})`,
          eb.fn<boolean>("has_cluster_privilege", [
            sql.raw("current_user"),
            eb.ref("c.name"),
            sql.lit("USAGE"),
          ]),
        ])
        .$castTo<boolean>()
        .as("canUse"),
    );
}

export type ClusterUsagePrivilegeParams = {
  clusterId: string;
};

export async function fetchClusterUsagePrivilege({
  params,
  queryKey,
  requestOptions,
}: {
  params: ClusterUsagePrivilegeParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  return executeSqlV2({
    queries: buildClusterUsagePrivilegeQuery(params.clusterId).compile(),
    queryKey,
    requestOptions,
  });
}
