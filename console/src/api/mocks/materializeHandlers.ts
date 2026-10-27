// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { http, HttpResponse } from "msw";

import { schemaObjectFilterQueryKeys } from "~/components/SchemaObjectFilter/useSchemaObjectFilters";
import { privilegesQueryKeys } from "~/hooks/usePrivileges";
import { clusterQueryKeys } from "~/platform/clusters/queries";
import { defaultRegionId } from "~/test/utils";

import { buildQueryKeyPart } from "../buildQueryKeySchema";
import { buildMockPrivilegesQueryHandler } from "./buildMockPrivilegesQueryHandler";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  buildUseSqlQueryHandler,
  mapKyselyToTabular,
} from "./buildSqlQueryHandler";

/**
 * If any MSW handler ever matches, but no response is returned, MSW will let the request
 * pass through and make a real request, which is not what we want. Instead, this
 * handler loudly fails when no handlers match.
 *
 * This handler must be the first one registered, so that it will be the last one called.
 */
export const fallthroughHandler = http.post("*/api/sql", async (info) => {
  const url = new URL(info.request.url);
  const queryKey = url.searchParams.get("query_key");
  const body = await info.request.clone().json();
  console.error("No SQL handler matched for statement:");
  console.error(body);
  // Return a json response so we can more easily display the error in test
  return HttpResponse.json(
    { message: "No SQL handler matched.", queryKey: queryKey },
    { status: 500 },
  );
});

export const fallthroughWebsocketHandler = http.get(
  "*/api/experimental/sql",
  async (info) => {
    const body = await info.request.clone().json();
    console.error("No Websocket handler matched for GET request:");
    console.error(body);
    // Return a json response so we can more easily display the error in test
    return HttpResponse.json(
      { message: "No Websocket handler matched." },
      { status: 500 },
    );
  },
);

export const successfulHealthCheckResponse = http.post("*/api/sql", () => {
  return HttpResponse.json({
    results: [
      {
        tag: "SELECT 1",
        rows: [["v0.88.1 (9f64f909a)"]],
        desc: {
          columns: [
            {
              name: "mz_version",
              type_oid: 25,
              type_len: -1,
              type_mod: -1,
            },
          ],
        },
        notices: [],
      },
    ],
  });
});

export const badRequestHandler = http.post("*/api/sql", () => {
  return new HttpResponse("bad request", { status: 400 });
});

const useSchemasHandler = buildUseSqlQueryHandler({
  type: "SELECT" as const,
  columns: ["id", "name", "database_id", "database_name"],
  rows: [],
});

const useSecretsHandler = buildUseSqlQueryHandler({
  type: "SELECT" as const,
  columns: ["id", "name", "created_at", "database_name", "schema_name"],
  rows: [],
});

const useClustersHandler = buildUseSqlQueryHandler({
  type: "SELECT" as const,
  columns: ["id", "cluster_name", "replica_id", "replica_name", "size"],
  rows: [],
});

// We cannot use privilegesQueryKeys.privileges directly since Jotai hasn't been initialized yet
const fetchPrivilegeTableHandlerQueryKey: ReturnType<
  (typeof privilegesQueryKeys)["privileges"]
> = [
  buildQueryKeyPart("privileges", {
    regionId: defaultRegionId,
    environmentVersion: undefined,
  }),
];

const fetchPrivilegeTableHandler = buildMockPrivilegesQueryHandler({
  queryKey: fetchPrivilegeTableHandlerQueryKey,
});

const availableClusterSizesQueryHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.availableClusterSizes(),
  results: [
    mapKyselyToTabular({
      rows: [
        {
          allowed_cluster_replica_sizes:
            '"3xsmall", "2xsmall", xsmall, small, medium, large, xlarge',
        },
      ],
      columns: buildColumns(["allowed_cluster_replica_sizes"]),
    }),
    mapKyselyToTabular({
      rows: [
        {
          size: "3xsmall",
        },
        {
          size: "2xsmall",
        },
      ],
      columns: buildColumns(["size"]),
    }),
  ],
});

const maxReplicasPerClusterQueryHandler = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.maxReplicasPerCluster(),
  results: mapKyselyToTabular({
    columns: buildColumns(["max_replicas_per_cluster"]),
    rows: [
      {
        max_replicas_per_cluster: "5",
      },
    ],
  }),
});

const schemaObjectFilterDatabaseListHandler = buildSqlQueryHandlerV2({
  queryKey: schemaObjectFilterQueryKeys.databaseList(),
  results: mapKyselyToTabular({
    columns: buildColumns(["id", "name"]),
    rows: [],
  }),
});

export default [
  availableClusterSizesQueryHandler,
  maxReplicasPerClusterQueryHandler,
  schemaObjectFilterDatabaseListHandler,
  useSchemasHandler,
  useSecretsHandler,
  useClustersHandler,
  fetchPrivilegeTableHandler,
  fallthroughWebsocketHandler,
  fallthroughHandler,
];
