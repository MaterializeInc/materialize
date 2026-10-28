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
import { delay, http, ws } from "msw";
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
import {
  mockSubscribeSocket,
  SubscribeSnapshot,
} from "~/test/mockSubscribeSocket";
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

  const lineageHandler = (clusterId: string) =>
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
    });

  const minutesAgo = (minutes: number) =>
    new Date(Date.now() - minutes * millisecondsInMinute);

  const sampleSnapshot = (occurredAt: Date): SubscribeSnapshot => ({
    columns: [
      buildColumn({ name: "replicaId" }),
      buildColumn({ name: "occurredAt" }),
      buildColumn({
        name: "cpuPercent",
        type_oid: MzDataType.float8,
      }),
    ],
    rows: [["u1", occurredAt.toISOString(), "0.5"]],
  });

  // Serves a 3h snapshot of one sample from 70 minutes ago, outside a 1h
  // window, while `subscribe.serve` is set.
  const serveHeldSample = (clusterId: string) => {
    const subscribe = { serve: true, requests: 0 };
    const occurredAt = minutesAgo(70);
    server.use(
      lineageHandler(clusterId),
      mockSubscribeSocket((sql) => {
        if (!sql.includes("utilization_overview_3h")) return undefined;
        subscribe.requests += 1;
        return subscribe.serve ? sampleSnapshot(occurredAt) : undefined;
      }),
    );
    return subscribe;
  };

  // Answers each cluster's 3h subscribe with `snapshotFor(clusterId)`, and
  // returns the clusters subscribed to, in order.
  const serveSubscribes = (
    clusterIds: string[],
    snapshotFor: (clusterId: string) => SubscribeSnapshot | undefined,
  ) => {
    const requests: string[] = [];
    server.use(
      ...clusterIds.map(lineageHandler),
      mockSubscribeSocket((sql, params) => {
        const clusterId = clusterIds.find((id) => params.includes(id));
        if (!sql.includes("utilization_overview_3h") || !clusterId) {
          return undefined;
        }
        requests.push(clusterId);
        return snapshotFor(clusterId);
      }),
    );
    return requests;
  };

  type WindowProps = { clusterId: string; timePeriodMinutes: number };

  const renderWindow = (
    initialProps: WindowProps,
    wrapper: React.ComponentType<React.PropsWithChildren>,
  ) => {
    const renders: LiveUtilization[] = [];
    const rendered = renderHook(
      ({ clusterId, timePeriodMinutes }: WindowProps) => {
        const result = useLiveClusterUtilization({
          clusterId,
          timePeriodMinutes,
          bucketSizeMs: millisecondsInMinute,
          tier: "unbinned3h",
          includeMemoryBreakdown: false,
        });
        renders.push(result);
        return result;
      },
      { wrapper, initialProps },
    );
    return { ...rendered, renders };
  };

  const renderOneHourWindow = (
    clusterId: string,
    wrapper: React.ComponentType<React.PropsWithChildren>,
  ) => renderWindow({ clusterId, timePeriodMinutes: 60 }, wrapper);

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

  it("keeps the subscribe when the window changes within a tier", async () => {
    const requests = serveSubscribes(["u11"], () =>
      sampleSnapshot(minutesAgo(30)),
    );
    const rendered = renderWindow(
      { clusterId: "u11", timePeriodMinutes: 15 },
      await createProviderWrapper(),
    );
    await waitFor(() => expect(rendered.result.current.data).toBeDefined());
    expect(rendered.result.current.data?.graphData).toEqual([]);

    const rendersBefore = rendered.renders.length;
    rendered.rerender({ clusterId: "u11", timePeriodMinutes: 60 });
    expect(rendered.result.current.data?.graphData).toHaveLength(1);
    // Room for a restarted session to send its subscribe.
    await new Promise((resolve) => setTimeout(resolve, 250));
    expect(requests).toEqual(["u11"]);
    for (const render of rendered.renders.slice(rendersBefore)) {
      expect(render.isRefreshing).toBe(false);
    }
  });

  it("serves no rows of the previous cluster after a cluster switch", async () => {
    const requests = serveSubscribes(["u12", "u13"], (clusterId) =>
      clusterId === "u12" ? sampleSnapshot(minutesAgo(30)) : undefined,
    );
    const rendered = renderOneHourWindow("u12", await createProviderWrapper());
    await waitFor(() =>
      expect(rendered.result.current.data?.graphData).toHaveLength(1),
    );

    const rendersBefore = rendered.renders.length;
    rendered.rerender({ clusterId: "u13", timePeriodMinutes: 60 });
    await waitFor(() => expect(requests).toEqual(["u12", "u13"]));
    const afterSwitch = rendered.renders.slice(rendersBefore);
    expect(afterSwitch.length).toBeGreaterThan(0);
    for (const render of afterSwitch) {
      expect(render).toMatchObject({ data: undefined, isLoading: true });
    }
  });

  it("holds rows as refreshing while a dropped socket resubscribes", async () => {
    let serve = true;
    const requests = serveSubscribes(["u14"], () =>
      serve ? sampleSnapshot(minutesAgo(30)) : undefined,
    );
    const sockets: Array<{ close: () => void }> = [];
    server.use(
      ws
        .link(/^wss?:\/\//)
        .addEventListener("connection", ({ client }) => sockets.push(client)),
    );
    const rendered = renderOneHourWindow("u14", await createProviderWrapper());
    await waitFor(() =>
      expect(rendered.result.current.data?.graphData).toHaveLength(1),
    );
    expect(rendered.result.current.isRefreshing).toBe(false);

    serve = false;
    sockets.forEach((socket) => socket.close());
    // The socket reopens after the connection manager's backoff.
    await waitFor(() => {
      expect(requests).toEqual(["u14", "u14"]);
      expect(rendered.result.current.isRefreshing).toBe(true);
    });
    expect(rendered.result.current).toMatchObject({
      isLoading: false,
      isError: false,
    });
    expect(rendered.result.current.data?.graphData).toHaveLength(1);
  });
});
