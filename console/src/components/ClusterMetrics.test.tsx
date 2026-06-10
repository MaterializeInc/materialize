// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { act, screen } from "@testing-library/react";
import React from "react";

import { ClusterReplicaMetricsResult } from "~/api/materialize/cluster/clusterReplicaMetrics";
import { ErrorCode, MzDataType } from "~/api/materialize/types";
import {
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { clusterMetricsQueryKey } from "~/queries/clusterReplicaMetrics";
import { allClusters } from "~/store/allClusters";
import {
  buildCluster,
  healthyEnvironment,
  renderComponent,
  RenderWithPathname,
  setFakeEnvironment,
} from "~/test/utils";

import { ClusterMetrics } from "./ClusterMetrics";

const clusterReplicaMetricsColumns = buildColumns([
  "id",
  "name",
  "size",
  { name: "cpuNanoCores", type_oid: MzDataType.uint8 },
  { name: "memoryBytes", type_oid: MzDataType.uint8 },
  { name: "diskBytes", type_oid: MzDataType.uint8 },
  { name: "heapBytes", type_oid: MzDataType.uint8 },
  { name: "cpuPercent", type_oid: MzDataType.float4 },
  { name: "memoryPercent", type_oid: MzDataType.float4 },
  { name: "diskPercent", type_oid: MzDataType.float4 },
  { name: "heapPercent", type_oid: MzDataType.float4 },
]);

function buildClusterMetrics(
  overrides?: Partial<ClusterReplicaMetricsResult[0]>,
): ClusterReplicaMetricsResult[0] {
  return {
    id: "u1",
    name: "r1",
    size: "2xsmall",
    cpuNanoCores: "500000000" as unknown as bigint,
    memoryBytes: "4294967296" as unknown as bigint,
    diskBytes: "28991029248" as unknown as bigint,
    heapBytes: "1073741824" as unknown as bigint,
    cpuPercent: "3.6954824" as unknown as number,
    memoryPercent: "2.5010833740234375" as unknown as number,
    diskPercent: "2.1920804624204284" as unknown as number,
    heapPercent: "25.0" as unknown as number,
    ...overrides,
  };
}

const validClusterMetricsHandler = buildSqlQueryHandlerV2({
  queryKey: clusterMetricsQueryKey({ clusterId: "u1" }),
  results: mapKyselyToTabular({
    rows: [buildClusterMetrics()],
    columns: clusterReplicaMetricsColumns,
  }),
});

describe("ClusterMetrics", () => {
  it("shows an error state when sources fail to fetch", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterMetricsQueryKey({ clusterId: "u1" }),
        results: {
          notices: [],
          error: {
            message: "Something went wrong",
            code: ErrorCode.INTERNAL_ERROR,
          },
        },
      }),
    );
    renderComponent(<ClusterMetrics clusterId="u1" clusterName="default" />, {
      initializeState: ({ set }) =>
        setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
    });
    expect(
      await screen.findByText("An error occurred loading cluster metrics."),
    ).toBeVisible();
  });

  it("displays cluster metrics", async () => {
    server.use(validClusterMetricsHandler);
    renderComponent(
      <RenderWithPathname>
        <ClusterMetrics clusterId="u1" clusterName="default" />
      </RenderWithPathname>,
      {
        initializeState: ({ set }) => {
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment);
          set(allClusters, {
            data: [buildCluster({ id: "u1", name: "default", disk: true })],
            error: undefined,
            snapshotComplete: true,
          });
        },
      },
    );

    expect(
      await screen.findByLabelText("Number of replicas"),
    ).toHaveTextContent("1 Replica");
    expect(await screen.findByLabelText("Replica size")).toHaveTextContent(
      "2xsmall",
    );

    // Memory Utilization
    expect(await screen.findByText("25%")).toBeVisible();
    expect(await screen.findByText("1 GB")).toBeVisible();

    // CPU
    expect(await screen.findByText("4%")).toBeVisible();
    expect(await screen.findByText("0.5 cores")).toBeVisible();

    // DISK
    expect(await screen.findByText("2%")).toBeVisible();
    expect(await screen.findByText("27 GB")).toBeVisible();

    act(() => screen.getByText("View cluster").click());
    expect(
      await screen.findByText("/regions/aws-us-east-1/clusters/u1/default"),
    ).toBeVisible();
  });

  it("does not display disk metrics if cluster has no disk", async () => {
    server.use(validClusterMetricsHandler);
    renderComponent(
      <RenderWithPathname>
        <ClusterMetrics clusterId="u1" clusterName="default" />
      </RenderWithPathname>,
      {
        initializeState: ({ set }) => {
          setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment);
          set(allClusters, {
            data: [buildCluster({ id: "u1", name: "default", disk: false })],
            error: undefined,
            snapshotComplete: true,
          });
        },
      },
    );

    await screen.findByLabelText("Number of replicas");
    expect(screen.queryByText("Disk")).not.toBeInTheDocument();
  });

  it("shows a message when metrics are not available", async () => {
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterMetricsQueryKey({ clusterId: "u1" }),
        results: mapKyselyToTabular({
          rows: [
            buildClusterMetrics({
              memoryPercent: null,
              cpuPercent: null,
              diskPercent: null,
              heapPercent: null,
            }),
          ],
          columns: clusterReplicaMetricsColumns,
        }),
      }),
    );
    renderComponent(<ClusterMetrics clusterId="u1" clusterName="default" />, {
      initializeState: ({ set }) => {
        setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment);
        set(allClusters, {
          data: [buildCluster({ id: "u1", name: "default", disk: false })],
          error: undefined,
          snapshotComplete: true,
        });
      },
    });
    expect(await screen.findByText("Cluster is warming up")).toBeVisible();
    expect(screen.getByText("Metrics will be available shortly")).toBeVisible();
  });
});
