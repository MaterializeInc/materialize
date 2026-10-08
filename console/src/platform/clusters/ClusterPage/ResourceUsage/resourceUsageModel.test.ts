// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { DataPoint } from "~/platform/clusters/ClusterOverview/types";

import {
  assignReplicaColors,
  canSwap,
  datumAt,
  hasOomBetween,
  memoryBar,
  mergeBuckets,
  mergedBucketMs,
  ramLimitPercent,
  replicasRestartedSince,
  sharedHeapLimitBytes,
  stateAt,
  StatusTransition,
  transformDdlEvents,
  transformReplicaTimeline,
} from "./resourceUsageModel";

const online = (at: number) => ({
  occurredAt: new Date(at),
  status: "online",
  reason: null,
});
const offline = (at: number, reason: string | null = null) => ({
  occurredAt: new Date(at),
  status: "offline",
  reason,
});

const mkPoint = (overrides: Partial<DataPoint> = {}): DataPoint => ({
  id: "u1",
  name: "r1",
  size: "100cc",
  bucketStart: 0,
  bucketEnd: 60,
  cpuPercent: null,
  memoryPercent: null,
  heapPercent: null,
  diskPercent: null,
  maxMemoryAndDiskPercent: null,
  swapPercent: null,
  ramLimitPercent: null,
  offlineEvents: [],
  ...overrides,
});

describe("transformReplicaTimeline", () => {
  it("starts in the state of the last transition before the window", () => {
    const timeline = transformReplicaTimeline({
      transitions: [online(-50), offline(40), online(60)],
      hydrationEpisodes: [],
      windowStartMs: 0,
      windowEndMs: 100,
    });
    expect(timeline.segments).toEqual([
      { state: "running", startMs: 0, endMs: 40 },
      { state: "offline", startMs: 40, endMs: 60 },
      { state: "running", startMs: 60, endMs: 100 },
    ]);
  });

  it("records an out-of-memory kill and restarts until back online", () => {
    const timeline = transformReplicaTimeline({
      transitions: [online(-10), offline(30, "oom-killed"), online(50)],
      hydrationEpisodes: [],
      windowStartMs: 0,
      windowEndMs: 100,
    });
    expect(timeline.oomAtMs).toEqual([30]);
    expect(stateAt(timeline, 40)).toBe("offline");
    expect(hasOomBetween(timeline, 0, 31)).toBe(true);
    expect(hasOomBetween(timeline, 31, 100)).toBe(false);
  });

  it("paints recorded hydration episodes over running time", () => {
    const timeline = transformReplicaTimeline({
      transitions: [offline(10), online(20)],
      hydrationEpisodes: [
        { startedAt: new Date(22), finishedAt: new Date(70) },
      ],
      windowStartMs: 0,
      windowEndMs: 100,
    });
    expect(timeline.segments).toEqual([
      { state: "offline", startMs: 10, endMs: 20 },
      { state: "running", startMs: 20, endMs: 22 },
      { state: "hydrating", startMs: 22, endMs: 70 },
      { state: "running", startMs: 70, endMs: 100 },
    ]);
  });

  it("ignores an episode with no finish time", () => {
    const timeline = transformReplicaTimeline({
      transitions: [online(-10)],
      hydrationEpisodes: [{ startedAt: new Date(20), finishedAt: null }],
      windowStartMs: 0,
      windowEndMs: 100,
    });
    expect(stateAt(timeline, 50)).toBe("running");
  });

  it("falls back to the sampled range when there are no transitions", () => {
    const timeline = transformReplicaTimeline({
      transitions: [],
      hydrationEpisodes: [],
      windowStartMs: 0,
      windowEndMs: 100,
      sampleRange: { startMs: 30, endMs: 80 },
    });
    expect(timeline.segments).toEqual([
      { state: "running", startMs: 30, endMs: 80 },
    ]);
  });

  it("ends a dropped replica's timeline at its window end", () => {
    const timeline = transformReplicaTimeline({
      transitions: [online(-10)],
      hydrationEpisodes: [],
      windowStartMs: 0,
      windowEndMs: 60,
    });
    expect(stateAt(timeline, 70)).toBeUndefined();
  });
});

describe("assignReplicaColors", () => {
  it("colors current replicas first, by name, cycling the palette", () => {
    const colors = assignReplicaColors(
      [
        { id: "u3", name: "r1", isCurrent: false },
        { id: "u2", name: "r2", isCurrent: true },
        { id: "u1", name: "r1", isCurrent: true },
      ],
      ["purple", "blue"],
    );
    expect(colors.get("u1")).toBe("purple");
    expect(colors.get("u2")).toBe("blue");
    expect(colors.get("u3")).toBe("purple");
  });
});

describe("memoryBar", () => {
  it("puts swap at the top of the heap bar", () => {
    expect(memoryBar(mkPoint({ heapPercent: 60, swapPercent: 20 }))).toEqual({
      heap: 60,
      swap: 20,
    });
  });

  it("draws heap alone when the environment doesn't report swap", () => {
    expect(memoryBar(mkPoint({ heapPercent: 40 }))).toEqual({
      heap: 40,
      swap: 0,
    });
  });

  it("never draws swap taller than the bar", () => {
    expect(memoryBar(mkPoint({ heapPercent: 10, swapPercent: 12 }))?.swap).toBe(
      10,
    );
  });

  it("draws nothing without a reading", () => {
    expect(memoryBar(mkPoint())).toBeUndefined();
  });
});

describe("ramLimitPercent", () => {
  it("marks where RAM ends when the replicas agree", () => {
    const series = [
      [
        mkPoint({ ramLimitPercent: 100 / 7 }),
        mkPoint({ ramLimitPercent: 100 / 7 }),
      ],
      [mkPoint({ ramLimitPercent: 100 })],
    ];
    expect(ramLimitPercent(series)).toBeCloseTo(100 / 7);
  });

  it("draws no line for sizes without swap", () => {
    expect(ramLimitPercent([[mkPoint({ ramLimitPercent: 100 })]])).toBe(
      undefined,
    );
    expect(canSwap(mkPoint({ ramLimitPercent: 100 }))).toBe(false);
  });

  it("draws no line when sizes put RAM in different places", () => {
    const series = [
      [mkPoint({ ramLimitPercent: 50 })],
      [mkPoint({ ramLimitPercent: 25 })],
    ];
    expect(ramLimitPercent(series)).toBeUndefined();
  });
});

describe("mergeBuckets", () => {
  it("keeps each metric's peak and the max-heap bucket's swap", () => {
    const merged = mergeBuckets(
      [
        mkPoint({
          bucketStart: 0,
          bucketEnd: 60,
          cpuPercent: 90,
          heapPercent: 40,
          swapPercent: 5,
        }),
        mkPoint({
          bucketStart: 60,
          bucketEnd: 120,
          cpuPercent: 30,
          heapPercent: 70,
          swapPercent: 20,
        }),
        mkPoint({
          bucketStart: 120,
          bucketEnd: 180,
          cpuPercent: 10,
          heapPercent: 50,
          swapPercent: 30,
        }),
      ],
      180,
    );
    expect(merged).toHaveLength(1);
    expect(merged[0]).toMatchObject({
      bucketStart: 0,
      bucketEnd: 180,
      cpuPercent: 90,
      heapPercent: 70,
      swapPercent: 20,
    });
  });

  it("aligns groups to time so they don't shift as buckets arrive", () => {
    const merged = mergeBuckets(
      [
        mkPoint({ bucketStart: 120, bucketEnd: 180 }),
        mkPoint({ bucketStart: 180, bucketEnd: 240 }),
      ],
      180,
    );
    expect(merged.map((point) => point.bucketStart)).toEqual([120, 180]);
  });
});

describe("mergedBucketMs", () => {
  const FIVE_MINUTES_MS = 5 * 60_000;
  const DAY_MS = 24 * 60 * 60_000;

  it("keeps the source width when bars already fit", () => {
    expect(
      mergedBucketMs({
        sourceBucketMs: 60_000,
        domainMs: 60 * 60_000,
        plotWidthPx: 900,
        replicaCount: 1,
        minSlotPx: 6,
      }),
    ).toBe(60_000);
  });

  it("widens a crowded day to a round multiple of the source", () => {
    // 288 five-minute buckets in 900px leave ~3px each, so 10 minutes.
    expect(
      mergedBucketMs({
        sourceBucketMs: FIVE_MINUTES_MS,
        domainMs: DAY_MS,
        plotWidthPx: 900,
        replicaCount: 1,
        minSlotPx: 6,
      }),
    ).toBe(10 * 60_000);
  });

  it("widens further when replicas share each bucket", () => {
    expect(
      mergedBucketMs({
        sourceBucketMs: FIVE_MINUTES_MS,
        domainMs: DAY_MS,
        plotWidthPx: 900,
        replicaCount: 3,
        minSlotPx: 6,
      }),
    ).toBe(30 * 60_000);
  });
});

describe("sharedHeapLimitBytes", () => {
  const LIMIT_BYTES = 24 * 2 ** 30;
  const limitsBySize = new Map([["100cc", LIMIT_BYTES]]);

  it("labels the line when every charted replica has the same size", () => {
    expect(sharedHeapLimitBytes(["100cc", "100cc"], limitsBySize)).toBe(
      LIMIT_BYTES,
    );
  });

  it("leaves the line unlabeled when charted sizes differ", () => {
    expect(
      sharedHeapLimitBytes(["100cc", "200cc"], limitsBySize),
    ).toBeUndefined();
  });

  it("leaves the line unlabeled when no running replica of the size reports a limit", () => {
    expect(sharedHeapLimitBytes(["200cc"], limitsBySize)).toBeUndefined();
  });
});

describe("replicasRestartedSince", () => {
  it("returns current replicas whose latest transition came online in the window", () => {
    const withId = (replicaId: string, t: StatusTransition) => ({
      ...t,
      replicaId,
    });
    const transitions = [
      withId("u1", online(-50)),
      withId("u2", offline(10)),
      withId("u2", online(20)),
      withId("u3", online(30)),
      withId("u4", online(40)),
      withId("u4", offline(60)),
    ];
    expect(
      replicasRestartedSince(transitions, new Set(["u1", "u2", "u4"]), 0),
    ).toEqual(["u2"]);
  });
});

describe("datumAt", () => {
  it("finds the bucket containing the time", () => {
    const data = [
      mkPoint({ bucketStart: 0, bucketEnd: 60, cpuPercent: 10 }),
      mkPoint({ bucketStart: 60, bucketEnd: 120, cpuPercent: 20 }),
    ];
    expect(datumAt(data, 60)?.cpuPercent).toBe(20);
    expect(datumAt(data, 120)).toBeUndefined();
  });
});

describe("transformDdlEvents", () => {
  it("names creation times from the objects, oldest first, dropping unknown ids", () => {
    const objects = [
      { id: "u10", name: "orders_idx", objectType: "index" },
      { id: "u11", name: "revenue", objectType: "materialized-view" },
    ];
    const events = transformDdlEvents(objects, [
      { id: "u11", occurredAt: new Date(200) },
      { id: "u10", occurredAt: new Date(100) },
      { id: "u99", occurredAt: new Date(150) },
      { id: null, occurredAt: new Date(160) },
    ]);
    expect(events).toEqual([
      { id: "u10", name: "orders_idx", objectType: "index", occurredAtMs: 100 },
      {
        id: "u11",
        name: "revenue",
        objectType: "materialized-view",
        occurredAtMs: 200,
      },
    ]);
  });
});
