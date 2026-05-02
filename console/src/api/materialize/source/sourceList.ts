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

import { extractEnvironmentVersion } from "~/api/buildQueryKeySchema";

import { executeSqlV2, queryBuilder } from "../";
import {
  buildFirstReplicaSourceStatisticsTable,
  getOwners,
} from "../expressionBuilders";

export type ListFilters = {
  clusterId?: string;
  databaseId?: string;
  nameFilter?: string;
  schemaId?: string;
  type?: string;
};

export type SourceListQueryParams = {
  filters: ListFilters;
  environmentVersion?: string;
};

/**
 * Builds a query to fetch all sources in the current environment
 */
export function buildSourceListQuery({
  filters: { databaseId, schemaId, nameFilter, clusterId, type },
  environmentVersion,
}: SourceListQueryParams) {
  let qb = queryBuilder
    .selectFrom("mz_sources as s")
    .innerJoin("mz_schemas as sc", "sc.id", "s.schema_id")
    .innerJoin("mz_databases as d", "d.id", "sc.database_id")
    .innerJoin("mz_clusters as c", "c.id", "s.cluster_id")
    .innerJoin("mz_object_lifetimes as ol", (join) =>
      join
        .onRef("ol.id", "=", "s.id")
        .on("ol.event_type", "=", "create")
        .on("ol.object_type", "=", "source"),
    )
    .innerJoin(getOwners().as("owners"), "owners.id", "s.owner_id")
    .innerJoin("mz_source_statuses as st", "st.id", "s.id")
    .leftJoin(
      buildFirstReplicaSourceStatisticsTable(environmentVersion).as("stat"),
      "stat.id",
      "s.id",
    )
    .leftJoin("mz_connections as cn", "cn.id", "connection_id")
    .leftJoin("mz_kafka_sources as ks", "ks.id", "s.id")
    .leftJoin("mz_webhook_sources as ws", "ws.id", "s.id")
    .select([
      "s.id",
      "s.name",
      "s.type",
      "s.size",
      "sc.name as schemaName",
      "d.name as databaseName",
      "st.status",
      "st.error",
      "stat.snapshot_committed as snapshotCommitted",
      "ol.occurred_at as createdAt",
      "c.id as clusterId",
      "c.name as clusterName",
      "cn.id as connectionId",
      "cn.name as connectionName",
      "ks.topic as kafkaTopic",
      "ws.url as webhookUrl",
      "owners.isOwner",
    ])
    .where("s.id", "like", "u%")
    .where("s.type", "<>", "subsource")
    .where("s.type", "<>", "progress")
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
  if (type) {
    qb = qb.where("s.type", "=", type);
  }

  return qb;
}

/**
 * Fetches all sources in the current environment
 */
export async function fetchSourceList({
  parameters,
  queryKey,
  requestOptions,
}: {
  parameters: SourceListQueryParams;
  queryKey: QueryKey;
  requestOptions: RequestInit;
}) {
  const environmentVersion = extractEnvironmentVersion(queryKey);

  const compiledQuery = buildSourceListQuery({
    ...parameters,
    environmentVersion,
  }).compile();
  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}

export type Source = InferResult<ReturnType<typeof buildSourceListQuery>>[0];
