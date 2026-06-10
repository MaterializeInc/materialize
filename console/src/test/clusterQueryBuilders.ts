// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import parsePostgresInterval from "postgres-interval";

import { Cluster, Replica } from "~/api/materialize/cluster/clusterList";
import {
  LagResults,
  MaterializationLagParams,
} from "~/api/materialize/cluster/materializationLag";
import { MzDataType } from "~/api/materialize/types";
import {
  buildColumn,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import { clusterQueryKeys } from "~/platform/clusters/queries";

export const clustersFetchColumns = [
  buildColumn({ name: "isOwner", type_oid: MzDataType.bool }),
  buildColumn({ name: "id" }),
  buildColumn({ name: "name" }),
  buildColumn({ name: "managed", type_oid: MzDataType.bool }),
  buildColumn({ name: "size" }),
  buildColumn({ name: "replicas", type_oid: MzDataType.jsonb }),
  buildColumn({ name: "latestStatusUpdate", type_oid: MzDataType.timestamptz }),
];

export function buildClusterServerResponse(
  overrides: Partial<Cluster> = {},
  replicas: Array<Replica> = [
    {
      id: "u678",
      name: "r1",
      size: "50cc",
      disk: true,
      statuses: [
        {
          replica_id: "u678",
          process_id: "0",
          reason: null,
          status: "online",
          updated_at: "2024-01-01T00:00:00.000Z",
        },
      ],
    },
  ],
) {
  return {
    isOwner: true,
    id: "s1",
    name: "default",
    size: "small",
    managed: true,
    replicas: replicas,
    latestStatusUpdate: "2024-01-01T00:00:00.000Z",
    ...overrides,
  } as Cluster;
}

export const emptyClustersResponse = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.list({
    includeSystemObjects: false,
  }),
  results: mapKyselyToTabular({
    columns: clustersFetchColumns,
    rows: [],
  }),
});

export const validClustersResponse = buildSqlQueryHandlerV2({
  queryKey: clusterQueryKeys.list(),
  results: mapKyselyToTabular({
    columns: clustersFetchColumns,
    rows: [
      buildClusterServerResponse({ id: "u1", name: "default" }),
      buildClusterServerResponse({ id: "u2", name: "quickstart" }),
      buildClusterServerResponse({ id: "s1", name: "mz_system" }),
      buildClusterServerResponse({ id: "s2", name: "mz_catalog_server" }),
    ],
  }),
});

export function buildValidMaterializationLagHandler(
  params: MaterializationLagParams,
  results: LagResults = [
    {
      type: "materialized-view",
      hydrated: true,
      lag: parsePostgresInterval("00:01:33.676"),
      targetObjectId: "u188",
      isOutdated: true,
    },
  ],
) {
  return buildSqlQueryHandlerV2({
    queryKey: clusterQueryKeys.materializationLag(params),
    results: mapKyselyToTabular({
      columns: clustersFetchColumns,
      rows: results,
    }),
  });
}
