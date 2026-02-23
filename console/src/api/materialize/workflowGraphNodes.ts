// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey } from "@tanstack/react-query";
import { InferResult, sql, SqlBool } from "kysely";

import { extractEnvironmentVersion } from "~/api/buildQueryKeySchema";
import { escapedLiteral as lit, executeSqlV2 } from "~/api/materialize";

import { queryBuilder } from "./db";
import { buildFirstReplicaSourceStatisticsTable } from "./expressionBuilders";

export type WorkflowGraphNodesParams = {
  objectIds: string[];
};

export type WorkflowGraphNodesQueryParams = WorkflowGraphNodesParams & {
  environmentVersion?: string;
};

/**
 * Fetches details for nodes of the workflow graph.
 */
export function buildWorkflowGraphNodesQuery({
  objectIds,
  environmentVersion,
}: WorkflowGraphNodesQueryParams) {
  return (
    queryBuilder
      .with("objects", (qb) =>
        qb
          .selectFrom("mz_objects as o")
          .innerJoin("mz_schemas as sc", "sc.id", "o.schema_id")
          .innerJoin("mz_databases as d", "d.id", "sc.database_id")
          .innerJoin("mz_object_lifetimes as ol", (join) =>
            join
              .onRef("ol.id", "=", "o.id")
              .on("ol.event_type", "=", "create")
              .on((eb) => eb("ol.object_type", "=", eb.ref("o.type"))),
          )
          .where(
            sql<SqlBool>`o.id in (${sql.join(objectIds.map((id) => lit(id)))})`,
          )
          .select([
            "o.id",
            "d.name as databaseName",
            "sc.name as schemaName",
            "o.name",
            "o.type",
            "ol.occurred_at as createdAt",
          ]),
      )
      // TODO: (#3400) Remove once all subsources are tables
      // We facade subsources as tables
      .with("tables_facaded", (qb) =>
        qb
          .selectFrom("objects as o")
          .leftJoin("mz_tables as t", "t.id", "o.id")
          .leftJoin("mz_sources as so", "so.id", "o.id")
          .where((eb) =>
            eb.or([
              eb("o.id", "=", eb.ref("t.id")),
              eb.and([
                eb("o.id", "=", eb.ref("so.id")),
                eb("so.type", "=", "subsource"),
              ]),
            ]),
          )
          .select([
            "o.id",
            "o.databaseName",
            "o.schemaName",
            "o.name",
            sql<string>`'table'`.as("type"),
            "o.createdAt",
          ]),
      )
      .with("tables", (qb) =>
        qb
          .selectFrom("tables_facaded as t")
          .leftJoin("mz_sources as so", "so.id", "t.id")
          .leftJoin("mz_source_statuses as sos", "sos.id", "so.id")
          .leftJoin(
            buildFirstReplicaSourceStatisticsTable(environmentVersion).as(
              "sst",
            ),
            "sst.id",
            "so.id",
          )
          .leftJoin("mz_connections as co", (join) =>
            join.on((eb) => eb("co.id", "=", eb.ref("so.connection_id"))),
          )
          .selectAll("t")
          .select([
            "so.cluster_id as clusterId",
            "so.type",
            "sos.status as sourceStatus",
            "sst.snapshot_committed as sourceSnapshotCommitted",
            sql<string>`${sql.lit(null)}::TEXT`.as("sinkStatus"),
            "co.id as connectionId",
            "co.name as connectionName",
          ]),
      )
      .with("sources", (qb) =>
        qb
          .selectFrom("objects as o")
          .innerJoin("mz_sources as so", "so.id", "o.id")
          .innerJoin("mz_source_statuses as sos", "sos.id", "so.id")
          .innerJoin(
            buildFirstReplicaSourceStatisticsTable(environmentVersion).as(
              "sst",
            ),
            "sst.id",
            "so.id",
          )
          .leftJoin("mz_connections as co", (join) =>
            join.on((eb) => eb("co.id", "=", eb.ref("so.connection_id"))),
          )
          // TODO: (#3400) Remove once all subsources are tables
          .where("so.type", "<>", "subsource")
          .selectAll("o")
          .select([
            "so.cluster_id as clusterId",
            "so.type as sourceType",
            "sos.status as sourceStatus",
            "sst.snapshot_committed as sourceSnapshotCommitted",
            sql<string>`${sql.lit(null)}::TEXT`.as("sinkStatus"),
            "co.id as connectionId",
            "co.name as connectionName",
          ]),
      )
      .with("indexes", (qb) =>
        qb
          .selectFrom("objects as o")
          .innerJoin("mz_indexes as i", "i.id", "o.id")
          .selectAll("o")
          .select([
            "i.cluster_id as clusterId",
            sql<string>`${sql.lit(null)}::TEXT`.as("sourceType"),
            sql<string>`${sql.lit(null)}::TEXT`.as("sourceStatus"),
            sql<string>`${sql.lit(null)}::BOOLEAN`.as(
              "sourceSnapshotCommitted",
            ),
            sql<string>`${sql.lit(null)}::TEXT`.as("sinkStatus"),
            sql<string>`${sql.lit(null)}::TEXT`.as("connectionId"),
            sql<string>`${sql.lit(null)}::TEXT`.as("connectionName"),
          ]),
      )
      .with("materialized_views", (qb) =>
        qb
          .selectFrom("objects as o")
          .innerJoin("mz_materialized_views as mv", "mv.id", "o.id")
          .selectAll("o")
          .select([
            "mv.cluster_id as clusterId",
            sql<string>`${sql.lit(null)}::TEXT`.as("sourceType"),
            sql<string>`${sql.lit(null)}::TEXT`.as("sourceStatus"),
            sql<string>`${sql.lit(null)}::BOOLEAN`.as(
              "sourceSnapshotCommitted",
            ),
            sql<string>`${sql.lit(null)}::TEXT`.as("sinkStatus"),
            sql<string>`${sql.lit(null)}::TEXT`.as("connectionId"),
            sql<string>`${sql.lit(null)}::TEXT`.as("connectionName"),
          ]),
      )
      .with("sinks", (qb) =>
        qb
          .selectFrom("objects as o")
          .innerJoin("mz_sinks as si", "si.id", "o.id")
          .innerJoin("mz_sink_statuses as sis", "sis.id", "si.id")
          .leftJoin("mz_connections as co", (join) =>
            join.on((eb) => eb("co.id", "=", eb.ref("si.connection_id"))),
          )
          .selectAll("o")
          .select([
            "si.cluster_id as clusterId",
            sql<string>`${sql.lit(null)}::TEXT`.as("sourceType"),
            sql<string>`${sql.lit(null)}::TEXT`.as("sourceStatus"),
            sql<string>`${sql.lit(null)}::BOOLEAN`.as(
              "sourceSnapshotCommitted",
            ),
            "sis.status as sinkStatus",
            "co.id as connectionId",
            "co.name as connectionName",
          ]),
      )
      .with("other_objects", (qb) =>
        qb
          .selectFrom("objects as o")
          .where("o.type", "<>", "source")
          .where("o.type", "<>", "sink")
          .where("o.type", "<>", "index")
          .where("o.type", "<>", "materialized-view")
          .where("o.type", "<>", "table")
          .selectAll("o")
          .select([
            sql<string>`${sql.lit(null)}::TEXT`.as("clusterId"),
            sql<string>`${sql.lit(null)}::TEXT`.as("sourceType"),
            sql<string>`${sql.lit(null)}::TEXT`.as("sourceStatus"),
            sql<string>`${sql.lit(null)}::BOOLEAN`.as(
              "sourceSnapshotCommitted",
            ),
            sql<string>`${sql.lit(null)}::TEXT`.as("sinkStatus"),
            sql<string>`${sql.lit(null)}::TEXT`.as("connectionId"),
            sql<string>`${sql.lit(null)}::TEXT`.as("connectionName"),
          ]),
      )
      .with("all", (qb) =>
        qb
          .selectFrom("sources")
          .union(qb.selectFrom("sinks").selectAll())
          .union(qb.selectFrom("indexes").selectAll())
          .union(qb.selectFrom("materialized_views").selectAll())
          .union(qb.selectFrom("tables").selectAll())
          .union(qb.selectFrom("other_objects").selectAll())
          .selectAll(),
      )
      .selectFrom("all as a")
      .leftJoin("mz_clusters as c", "c.id", "a.clusterId")
      .selectAll("a")
      .select(["c.name as clusterName"])
  );
}

export type WorkflowGraphNode = InferResult<
  ReturnType<typeof buildWorkflowGraphNodesQuery>
>[0];

/**
 * Fetches details for nodes of the workflow graph.
 */
export async function fetchWorkflowGraphNodes({
  params,
  queryKey,
  requestOptions,
}: {
  params: WorkflowGraphNodesQueryParams;
  queryKey: QueryKey;
  requestOptions?: RequestInit;
}) {
  const environmentVersion = extractEnvironmentVersion(queryKey);
  const compiledQuery = buildWorkflowGraphNodesQuery({
    ...params,
    environmentVersion,
  }).compile();

  return executeSqlV2({
    queries: compiledQuery,
    queryKey: queryKey,
    requestOptions,
  });
}
