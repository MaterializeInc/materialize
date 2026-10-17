// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { renderHook, waitFor } from "@testing-library/react";
import { millisecondsInMinute } from "date-fns/constants";
import { delay, http } from "msw";
import React from "react";

import { BinnedSubscribeRow } from "~/api/materialize/cluster/replicaUtilizationBinning";
import { MzDataType } from "~/api/materialize/types";
import {
  buildColumn,
  buildColumns,
  buildSqlQueryHandlerV2,
  mapKyselyToTabular,
} from "~/api/mocks/buildSqlQueryHandler";
import server from "~/api/mocks/server";
import { clusterQueryKeys } from "~/platform/clusters/queries";
import { buildQueryClient } from "~/queryClient";
import { mockSubscribeSocket } from "~/test/mockSubscribeSocket";
import { createProviderWrapper } from "~/test/utils";

import {
  StoredSample,
  transformLiveUtilization,
  useLiveClusterUtilization,
} from "./useLiveClusterUtilization";
const END_MS = Date.UTC(2026, 9, 2, 12, 0);

const sample = (minutesAgo: number, cpuPercent: number): StoredSample => ({
  replicaId: "u1",
  clusterId: "u9",
  size: "100cc",
  name: "r1",
  occurredAt: new Date(
    END_MS - minutesAgo * millisecondsInMinute,
  ).toISOString(),
  cpuPercent,
  memoryPercent: 0.4,
  diskPercent: null,
  heapPercent: 0.5,
  memoryAndDiskPercent: null,
});

const bucket = (minutesAgo: number, cpuPercent: number): BinnedSubscribeRow => {
  const start = new Date(
    END_MS - minutesAgo * millisecondsInMinute,
  ).toISOString();
  return {
    bucketStart: start,
    bucketEnd: new Date(
      END_MS - (minutesAgo - 5) * millisecondsInMinute,
    ).toISOString(),
    replicaId: "u1",
    clusterId: "u9",
    size: "100cc",
    name: "r1",
    maxMemoryPercent: 0.4,
    maxMemoryAt: start,
    maxDiskPercent: null,
    maxDiskAt: start,
    maxCpuPercent: cpuPercent,
    maxCpuAt: start,
    maxHeapPercent: 0.5,
    maxHeapAt: start,
    maxMemoryAndDiskPercent: null,
    maxMemoryAndDiskMemoryPercent: null,
    maxMemoryAndDiskDiskPercent: null,
    maxMemoryAndDiskAt: start,
    offlineEvents: null,
  };
};

const cpuSeries = (result: ReturnType<typeof transformLiveUtilization>) =>
  result.graphData[0].data.map((point) => point.cpuPercent);

describe("transformLiveUtilization", () => {
  it("bins 3h samples into the window's buckets, keeping each bucket's peak", () => {
    const result = transformLiveUtilization({
      rows: {
        tier: "unbinned3h",
        // The 3h subscribe holds samples from before a 1h window too.
        samples: [sample(90, 0.9), sample(30, 0.2), sample(29.5, 0.6)],
      },
      endDate: new Date(END_MS),
      timePeriodMinutes: 60,
      bucketSizeMs: millisecondsInMinute,
    });
    expect(cpuSeries(result)).toEqual([60]);
  });

  it("clips 24h buckets to the window and puts them in time order", () => {
    const result = transformLiveUtilization({
      rows: {
        tier: "binned24h",
        buckets: [bucket(60, 0.3), bucket(600, 0.9), bucket(120, 0.1)],
      },
      endDate: new Date(END_MS),
      timePeriodMinutes: 6 * 60,
      bucketSizeMs: 5 * millisecondsInMinute,
    });
    expect(cpuSeries(result)).toEqual([10, 30]);
  });
});

describe("useLiveClusterUtilization", () => {
  type LiveUtilization = ReturnType<typeof useLiveClusterUtilization>;

  // Serves a 3h snapshot of one sample from 70 minutes ago, outside a 1h
  // window, while `subscribe.serve` is set.
  const serveHeldSample = (clusterId: string) => {
    const subscribe = { serve: true, requests: 0 };
    const occurredAt = new Date(Date.now() - 70 * millisecondsInMinute);
    server.use(
      buildSqlQueryHandlerV2({
        queryKey: clusterQueryKeys.deploymentLineage({
          clusterIdsKey: clusterId,
        }),
        results: mapKyselyToTabular({
          columns: buildColumns([
            "clusterId",
            "currentDeploymentClusterId",
            "clusterName",
          ]),
          rows: [],
        }),
      }),
      mockSubscribeSocket((sql) => {
        if (!sql.includes("utilization_overview_3h")) return undefined;
        subscribe.requests += 1;
        return subscribe.serve
          ? {
              columns: [
                buildColumn({ name: "replicaId" }),
                buildColumn({ name: "occurredAt" }),
                buildColumn({
                  name: "cpuPercent",
                  type_oid: MzDataType.float8,
                }),
              ],
              rows: [["u1", occurredAt.toISOString(), "0.5"]],
            }
          : undefined;
      }),
    );
    return subscribe;
  };

  const renderOneHourWindow = (
    clusterId: string,
    wrapper: React.ComponentType<React.PropsWithChildren>,
  ) => {
    const renders: LiveUtilization[] = [];
    const rendered = renderHook(
      () => {
        const result = useLiveClusterUtilization({
          clusterId,
          timePeriodMinutes: 60,
          bucketSizeMs: millisecondsInMinute,
          tier: "unbinned3h",
          includeMemoryBreakdown: false,
        });
        renders.push(result);
        return result;
      },
      { wrapper },
    );
    return { ...rendered, renders };
  };

  // Held rows outside the window must never read as a window with no usage.
  const expectEveryRenderAwaitingRows = (renders: LiveUtilization[]) => {
    expect(renders.length).toBeGreaterThan(0);
    for (const render of renders) {
      expect(render).toMatchObject({
        data: undefined,
        isLoading: true,
        isRefreshing: true,
      });
    }
  };

  it("waits for a new snapshot when held rows have left the window", async () => {
    const subscribe = serveHeldSample("u9");
    const ProviderWrapper = await createProviderWrapper();

    const first = renderOneHourWindow("u9", ProviderWrapper);
    await waitFor(() => expect(first.result.current.data).toBeDefined());
    expect(first.result.current.data?.graphData).toEqual([]);
    first.unmount();

    // Coming back, the held sample is still outside the window and the new
    // subscribe hasn't delivered yet.
    subscribe.serve = false;
    const second = renderOneHourWindow("u9", ProviderWrapper);
    await waitFor(() => expect(subscribe.requests).toEqual(2));
    expectEveryRenderAwaitingRows(second.renders);
  });

  it("holds rows as refreshing while an evicted lineage is refetched", async () => {
    serveHeldSample("u10");
    const first = renderOneHourWindow("u10", await createProviderWrapper());
    await waitFor(() => expect(first.result.current.data).toBeDefined());
    first.unmount();

    // A fresh query cache has no lineage, and its refetch never returns, so
    // no subscribe starts.
    let lineageRequested = false;
    server.use(
      http.post("*/api/sql", async () => {
        lineageRequested = true;
        await delay("infinite");
      }),
    );
    const second = renderOneHourWindow(
      "u10",
      await createProviderWrapper({ queryClient: buildQueryClient() }),
    );
    await waitFor(() => expect(lineageRequested).toBe(true));
    expectEveryRenderAwaitingRows(second.renders);
  });
});
