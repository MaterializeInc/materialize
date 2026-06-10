// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Cluster, Replica } from "~/api/materialize/cluster/clusterList";
import {
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import { clustersFetchColumns } from "~/test/clusterQueryBuilders";

import { clusterQueryKeys } from "./queries";

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

/**
 * Used to setup the initial route and handler for nested detail pages
 */
export function detailPageSetupHelpers() {
  const detailPageCluster: Cluster = {
    id: "u2",
    name: "default",
    size: "50cc",
    managed: true,
    disk: true,
    replicas: [
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
    latestStatusUpdate: "2024-01-01T00:00:00.000Z",
  };

  const detailPageSetupHandler = buildSqlQueryHandlerV2({
    queryKey: clusterQueryKeys.list(),
    results: mapKyselyToTabular({
      columns: clustersFetchColumns,
      rows: [buildClusterServerResponse({ id: "u2", name: "default" })],
    }),
  });

  const detailPageInitialRouteEntries = [
    `/${detailPageCluster.id}/${detailPageCluster.name}`,
  ];

  return {
    detailPageSetupHandler,
    detailPageInitialRouteEntries,
    detailPageCluster,
  };
}
