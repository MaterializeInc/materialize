// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult } from "kysely";

import { executeSqlV2, queryBuilder } from "../";
import { getOwners } from "../expressionBuilders";

export type ListFilters = {
  clusterId?: string;
  databaseId?: string;
  nameFilter?: string;
  schemaId?: string;
};

export function buildSinkListQuery({
  databaseId,
  schemaId,
  nameFilter,
  clusterId,
}: ListFilters) {
  let qb = queryBuilder
    .selectFrom("mz_sinks as s")
    .innerJoin("mz_schemas as sc", "sc.id", "s.schema_id")
    .innerJoin("mz_databases as d", "d.id", "sc.database_id")
    .innerJoin("mz_clusters as c", "c.id", "s.cluster_id")
    .innerJoin(getOwners().as("owners"), "owners.id", "s.owner_id")
    .innerJoin("mz_object_lifetimes as ol", (join) =>
      join
        .onRef("ol.id", "=", "s.id")
        .on("ol.event_type", "=", "create")
        .on("ol.object_type", "=", "sink"),
    )
    .innerJoin("mz_sink_statuses as st", "st.id", "s.id")
    .leftJoin("mz_connections as cn", "cn.id", "connection_id")
    .leftJoin("mz_kafka_sinks as ks", "ks.id", "s.id")
    .select([
      "s.id",
      "s.name",
      "s.type",
      "s.size",
      "sc.name as schemaName",
      "d.name as databaseName",
      "ol.occurred_at as createdAt",
      "c.id as clusterId",
      "c.name as clusterName",
      "cn.id as connectionId",
      "cn.name as connectionName",
      "ks.topic as kafkaTopic",
      "st.status",
      "st.error",
      "owners.isOwner",
    ])
    .orderBy(["d.name", "sc.name", "name"]);
  if (databaseId) {
    qb = qb.where("d.id", "=", databaseId);
  }
  if (schemaId) {
    qb = qb.where("sc.id", "=", schemaId);
  }
  if (nameFilter) {
    qb = qb.where("s.name", "like", `%${nameFilter}%`);
  }
  if (clusterId) {
    qb = qb.where("s.cluster_id", "=", clusterId);
  }

  return qb;
}

export async function fetchSinkList({
  filters,
  queryKey,
  requestOptions,
}: {
  filters: ListFilters;
  queryKey: QueryKey;
  requestOptions: RequestInit;
}) {
  const queries = buildSinkListQuery(filters).compile();
  return executeSqlV2({ queries, queryKey, requestOptions });
}

export type Sink = InferResult<ReturnType<typeof buildSinkListQuery>>[number];
