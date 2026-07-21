// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Bucket,
  bucketRowsToBucketsByReplicaId,
  maxByMetric,
  OfflineEvent,
  rebucketUtilizationSamples,
  toReplicaUtilizationGraphData,
  UtilizationBucketRow,
  UtilizationSample,
} from "./replicaUtilizationBinning";

// bucketRowsToBucketsByReplicaId reports malformed rows to Sentry before throwing;
// stub it so the throw test doesn't depend on a Sentry client.
vi.mock("@sentry/react");

const MINUTE = 60_000;

function mkSample(
  overrides: Partial<UtilizationSample> = {},
): UtilizationSample {
  return {
    replicaId: "u1",
    clusterId: "c1",
    size: "small",
    name: "r1",
    occurredAt: new Date(0),
    cpuPercent: 0,
    memoryPercent: 0,
    diskPercent: 0,
    heapPercent: 0,
    memoryAndDiskPercent: 0,
    ...overrides,
  };
}

function mkBucketRow(
  overrides: Partial<UtilizationBucketRow> = {},
): UtilizationBucketRow {
  return {
    bucketStart: new Date(0),
    bucketEnd: new Date(MINUTE),
    replicaId: "u1",
    clusterId: "c1",
    size: "small",
    name: "r1",
    maxMemoryPercent: 0.5,
    maxMemoryAt: new Date(0),
    maxDiskPercent: 0.1,
    maxDiskAt: new Date(0),
    maxCpuPercent: 0.2,
    maxCpuAt: new Date(0),
    maxHeapPercent: 0.3,
    maxHeapAt: new Date(0),
    maxMemoryAndDiskPercent: 0.4,
    maxMemoryAndDiskMemoryPercent: 0.5,
    maxMemoryAndDiskDiskPercent: 0.1,
    maxMemoryAndDiskAt: new Date(0),
    offlineEvents: null,
    ...overrides,
  };
}

function mkBucket(overrides: Partial<Bucket> = {}): Bucket {
  return {
    size: "small",
    bucketStart: new Date(0),
    replicaId: "u1",
    bucketEnd: new Date(MINUTE),
    name: "r1",
    currentDeploymentClusterId: "c1",
    clusterId: "c1",
    maxMemory: { percent: 0.1, occurredAt: new Date(0) },
    maxDisk: { percent: 0.1, occurredAt: new Date(0) },
    maxCpu: { percent: 0.1, occurredAt: new Date(0) },
    maxHeap: { percent: 0.1, occurredAt: new Date(0) },
    maxMemoryAndDisk: {
      percent: 0.1,
      memoryPercent: 0.1,
      diskPercent: 0.1,
      occurredAt: new Date(0),
    },
    offlineEvents: null,
    ...overrides,
  };
}

describe("maxByMetric", () => {
  it("returns the sample with the highest metric value", () => {
    const samples = [
      mkSample({ cpuPercent: 0.1 }),
      mkSample({ cpuPercent: 0.9 }),
      mkSample({ cpuPercent: 0.5 }),
    ];
    expect(maxByMetric(samples, (s) => s.cpuPercent)).toBe(samples[1]);
  });

  it("treats null as the lowest value", () => {
    const samples = [
      mkSample({ cpuPercent: null }),
      mkSample({ cpuPercent: 0.2 }),
    ];
    expect(maxByMetric(samples, (s) => s.cpuPercent)).toBe(samples[1]);
  });
});

describe("rebucketUtilizationSamples", () => {
  it("buckets samples into epoch-aligned windows and takes the max per metric", () => {
    const samples = [
      mkSample({
        occurredAt: new Date(1_000),
        cpuPercent: 0.2,
        memoryPercent: 0.6,
      }),
      mkSample({
        occurredAt: new Date(2_000),
        cpuPercent: 0.8,
        memoryPercent: 0.1,
      }),
    ];
    const rows = rebucketUtilizationSamples(samples, MINUTE, 0);
    expect(rows).toHaveLength(1);
    expect(rows[0].bucketStart).toEqual(new Date(0));
    expect(rows[0].bucketEnd).toEqual(new Date(MINUTE));
    expect(rows[0].maxCpuPercent).toBe(0.8);
    expect(rows[0].maxMemoryPercent).toBe(0.6);
  });

  it("drops samples before the window start", () => {
    const samples = [
      mkSample({ occurredAt: new Date(500) }),
      mkSample({ occurredAt: new Date(5_000) }),
    ];
    const rows = rebucketUtilizationSamples(samples, MINUTE, 1_000);
    expect(rows).toHaveLength(1);
    expect(rows[0].bucketStart).toEqual(new Date(0));
  });

  it("splits a replica's samples across multiple buckets, maxing within each", () => {
    const samples = [
      // First bucket [0, MINUTE): the higher of two samples wins.
      mkSample({ occurredAt: new Date(10_000), cpuPercent: 0.3 }),
      mkSample({ occurredAt: new Date(50_000), cpuPercent: 0.7 }),
      // A sample exactly on the boundary belongs to the second bucket.
      mkSample({ occurredAt: new Date(MINUTE), cpuPercent: 0.4 }),
      mkSample({ occurredAt: new Date(2 * MINUTE + 1_000), cpuPercent: 0.9 }),
    ];
    const rows = rebucketUtilizationSamples(samples, MINUTE, 0);
    expect(rows).toHaveLength(3);
    expect(rows.map((r) => r.bucketStart.getTime())).toEqual([
      0,
      MINUTE,
      2 * MINUTE,
    ]);
    expect(rows.map((r) => r.maxCpuPercent)).toEqual([0.7, 0.4, 0.9]);
    expect(rows[0].maxCpuAt).toEqual(new Date(50_000));
  });

  it("separates buckets per replica and sorts by bucket start", () => {
    const samples = [
      mkSample({ replicaId: "u2", occurredAt: new Date(MINUTE + 1_000) }),
      mkSample({ replicaId: "u1", occurredAt: new Date(1_000) }),
    ];
    const rows = rebucketUtilizationSamples(samples, MINUTE, 0);
    expect(rows).toHaveLength(2);
    expect(rows[0].replicaId).toBe("u1");
    expect(rows[0].bucketStart.getTime()).toBeLessThan(
      rows[1].bucketStart.getTime(),
    );
  });
});

describe("bucketRowsToBucketsByReplicaId", () => {
  it("groups rows by replica and tracks the overall min/max bounds", () => {
    const rows = [
      mkBucketRow({ bucketStart: new Date(0), bucketEnd: new Date(1_000) }),
      mkBucketRow({ bucketStart: new Date(1_000), bucketEnd: new Date(2_000) }),
      mkBucketRow({
        replicaId: "u2",
        bucketStart: new Date(500),
        bucketEnd: new Date(1_500),
      }),
    ];
    const { bucketsByReplicaId, minBucketStartMs, maxBucketEndMs } =
      bucketRowsToBucketsByReplicaId(rows);
    expect(Object.keys(bucketsByReplicaId).sort()).toEqual(["u1", "u2"]);
    expect(bucketsByReplicaId.u1).toHaveLength(2);
    expect(minBucketStartMs).toBe(0);
    expect(maxBucketEndMs).toBe(2_000);
  });

  it("defaults currentDeploymentClusterId to the row's clusterId without a resolver", () => {
    const { bucketsByReplicaId } = bucketRowsToBucketsByReplicaId([
      mkBucketRow({ clusterId: "c1" }),
    ]);
    expect(bucketsByReplicaId.u1[0].currentDeploymentClusterId).toBe("c1");
  });

  it("resolves currentDeploymentClusterId through the resolver", () => {
    const { bucketsByReplicaId } = bucketRowsToBucketsByReplicaId(
      [mkBucketRow({ clusterId: "past" })],
      (id) => (id === "past" ? "current" : undefined),
    );
    expect(bucketsByReplicaId.u1[0].currentDeploymentClusterId).toBe("current");
  });

  it("throws when a row is missing name or clusterId", () => {
    expect(() =>
      bucketRowsToBucketsByReplicaId([mkBucketRow({ name: null })]),
    ).toThrow();
  });
});

describe("toReplicaUtilizationGraphData", () => {
  it("scales bucket percents to a 0-100 range per replica", () => {
    const data = {
      bucketsByReplicaId: {
        u1: [mkBucket({ maxCpu: { percent: 0.5, occurredAt: new Date(0) } })],
      },
      minBucketStartMs: 0,
      maxBucketEndMs: MINUTE,
    };
    const result = toReplicaUtilizationGraphData(
      data,
      new Date(0),
      new Date(MINUTE),
    );
    expect(result.graphData[0].id).toBe("u1");
    expect(result.graphData[0].data[0].cpuPercent).toBe(50);
  });

  it("widens the axis to the data bounds and to the selected window", () => {
    const data = {
      bucketsByReplicaId: { u1: [mkBucket()] },
      minBucketStartMs: 1_000,
      maxBucketEndMs: 9_000,
    };

    // Selected window narrower than the data: axis grows out to the data bounds.
    const narrow = toReplicaUtilizationGraphData(
      data,
      new Date(3_000),
      new Date(7_000),
    );
    expect(narrow.startDate).toEqual(new Date(1_000));
    expect(narrow.endDate).toEqual(new Date(9_000));

    // Selected window wider than the data: axis keeps the selected window.
    const wide = toReplicaUtilizationGraphData(
      data,
      new Date(0),
      new Date(20_000),
    );
    expect(wide.startDate).toEqual(new Date(0));
    expect(wide.endDate).toEqual(new Date(20_000));
  });

  it("collects only offline/not-ready events that are not oom-killed", () => {
    const offlineEvents: OfflineEvent[] = [
      {
        replicaId: "u1",
        occurredAt: "1970-01-01T00:00:00Z",
        status: "offline",
        reason: "crashed",
      },
      {
        replicaId: "u1",
        occurredAt: "1970-01-01T00:00:00Z",
        status: "offline",
        reason: "oom-killed",
      },
      {
        replicaId: "u1",
        occurredAt: "1970-01-01T00:00:00Z",
        status: "online",
        reason: "whatever",
      },
    ];
    const data = {
      bucketsByReplicaId: { u1: [mkBucket({ offlineEvents })] },
      minBucketStartMs: 0,
      maxBucketEndMs: MINUTE,
    };
    const result = toReplicaUtilizationGraphData(
      data,
      new Date(0),
      new Date(MINUTE),
    );
    expect(result.offlineEvents).toHaveLength(1);
    expect(result.offlineEvents[0].offlineReason).toBe("crashed");
  });
});
