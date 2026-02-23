// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { sql } from "kysely";
import React from "react";

import { hasSuperUserPrivileges } from "~/api/materialize/expressionBuilders";

import { queryBuilder } from "../db";
import useSqlTyped from "../useSqlTyped";

export function buildConnectorClustersQuery() {
  return (
    queryBuilder
      .selectFrom("mz_clusters as c")
      .select(["c.id", "c.name"])
      // system clusters start with `s`
      .where("c.id", "like", "u%")
      .where("c.id", "not in", (qb) =>
        qb
          .selectFrom("mz_cluster_replicas as r")
          .select("cluster_id")
          .groupBy("cluster_id")
          .having((eb) => sql`${eb.fn.count("id")} > 1`),
      )
      .where((eb) =>
        eb.or([
          sql<boolean>`(${hasSuperUserPrivileges()})`,
          eb.fn<boolean>("has_cluster_privilege", [
            sql.raw("current_user"),
            eb.ref("c.name"),
            sql.lit("CREATE"),
          ]),
        ]),
      )
      .compile()
  );
}

export const connectorClustersColumns = ["id", "name"];

/**
 * Fetches clusters that are valid targets for sources and sinks
 * Sources and sinks cannot be installed on:
 * - system clusters
 * - clusters that also have indexes or materialized views on them
 * - clusters that have more than one replica
 * - we also don't allow the default cluster to be selected, since this would be
 *   confusing, as you would have nowhere to run queries
 */
function useConnectorClusters() {
  const query = React.useMemo(() => buildConnectorClustersQuery(), []);

  return useSqlTyped(query);
}

export type ClusterResponse = ReturnType<typeof useConnectorClusters>;

export type Cluster = NonNullable<ClusterResponse["results"]>[0];

export default useConnectorClusters;
