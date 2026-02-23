// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { queryBuilder } from "~/api/materialize";
import useSqlTyped from "~/api/materialize/useSqlTyped";

/**
 * Fetches all materialized views for a given cluster
 */
function useMaterializedViews({
  clusterId,
  databaseId,
  schemaId,
  nameFilter,
}: {
  clusterId?: string;
  databaseId?: string;
  schemaId?: string;
  nameFilter?: string;
} = {}) {
  const query = React.useMemo(() => {
    let qb = queryBuilder
      .selectFrom("mz_materialized_views as mv")
      .innerJoin("mz_schemas as sc", "sc.id", "mv.schema_id")
      .innerJoin("mz_databases as d", "d.id", "sc.database_id")
      .select([
        "mv.id",
        "mv.name",
        "mv.definition",
        "d.name as databaseName",
        "sc.name as schemaName",
      ])
      .where("mv.id", "like", "u%")
      .orderBy(["d.name", "sc.name", "name"]);
    if (databaseId) {
      qb = qb.where("d.id", "=", databaseId);
    }
    if (schemaId) {
      qb = qb.where("sc.id", "=", schemaId);
    }
    if (nameFilter) {
      qb = qb.where("mv.name", "like", `%${nameFilter}%`);
    }
    if (clusterId) {
      qb = qb.where("mv.cluster_id", "=", clusterId);
    }
    return qb.compile();
  }, [clusterId, databaseId, nameFilter, schemaId]);

  const response = useSqlTyped(query);

  return { ...response, data: response.results };
}

export type MaterializedViewsResponse = ReturnType<typeof useMaterializedViews>;

export type MaterializedView = NonNullable<
  MaterializedViewsResponse["results"]
>[0];

export { useMaterializedViews };
