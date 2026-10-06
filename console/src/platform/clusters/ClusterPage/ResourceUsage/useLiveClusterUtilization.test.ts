// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { BinnedSubscribeRow } from "~/api/materialize/cluster/replicaUtilizationBinning";

import {
  StoredSample,
  transformLiveUtilization,
} from "./useLiveClusterUtilization";

const MINUTE_MS = 60_000;
const END_MS = Date.UTC(2026, 9, 2, 12, 0);

const sample = (minutesAgo: number, cpuPercent: number): StoredSample => ({
  replicaId: "u1",
  clusterId: "u9",
  size: "100cc",
  name: "r1",
  occurredAt: new Date(END_MS - minutesAgo * MINUTE_MS).toISOString(),
  cpuPercent,
  memoryPercent: 0.4,
  diskPercent: null,
  heapPercent: 0.5,
  memoryAndDiskPercent: null,
});

const bucket = (minutesAgo: number, cpuPercent: number): BinnedSubscribeRow => {
  const start = new Date(END_MS - minutesAgo * MINUTE_MS).toISOString();
  return {
    bucketStart: start,
    bucketEnd: new Date(END_MS - (minutesAgo - 5) * MINUTE_MS).toISOString(),
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
      bucketSizeMs: MINUTE_MS,
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
      bucketSizeMs: 5 * MINUTE_MS,
    });
    expect(cpuSeries(result)).toEqual([10, 30]);
  });
});
