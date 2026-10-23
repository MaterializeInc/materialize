// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  findUnfinishedHydrations,
  heapLimitBytes,
  Hydration,
  HydrationReplica,
  mergeHydrationReplicas,
  replicaBootTimes,
  ReplicaTransition,
  summarizeObjectSpans,
  transformHydrations,
  transformObjectHydrationSpans,
  typicalFullHydration,
} from "./hydrationModel";

const at = (time: string) => new Date(`2026-10-20T${time}Z`);

const replica = (
  overrides: Partial<HydrationReplica> & { id: string },
): HydrationReplica => ({
  name: "r1",
  size: "100cc",
  isCurrent: true,
  createdAt: undefined,
  droppedAt: undefined,
  ...overrides,
});

const episode = (
  replicaId: string,
  startedAt: string,
  finishedAt: string,
  objectCount = 8,
) => ({
  replicaId,
  startedAt: at(startedAt),
  finishedAt: at(finishedAt),
  objectCount,
  peakHeapBytes: 100,
  peakMemoryBytes: 90,
  peakDiskBytes: 10,
});

const transition = (
  replicaId: string,
  time: string,
  status: string,
  reason: string | null = null,
): ReplicaTransition => ({
  replicaId,
  occurredAt: at(time),
  status,
  reason,
});

describe("mergeHydrationReplicas", () => {
  it("lists current replicas with their current names, then dropped ones newest first", () => {
    const replicas = mergeHydrationReplicas(
      [{ id: "u3", name: "r1-renamed", size: "100cc" }],
      [
        {
          replicaId: "u1",
          name: "r1",
          size: "50cc",
          createdAt: at("01:00:00"),
          droppedAt: at("10:40:00"),
        },
        {
          replicaId: "u2",
          name: "r1",
          size: "50cc",
          createdAt: at("05:00:00"),
          droppedAt: at("10:40:00"),
        },
        {
          replicaId: "u3",
          name: "r1",
          size: "100cc",
          createdAt: at("10:40:00"),
          droppedAt: null,
        },
      ],
    );

    expect(
      replicas.map(({ id, name, size, isCurrent, createdAt }) => [
        id,
        name,
        size,
        isCurrent,
        createdAt?.toISOString(),
      ]),
    ).toEqual([
      ["u3", "r1-renamed", "100cc", true, "2026-10-20T10:40:00.000Z"],
      ["u2", "r1", "50cc", false, "2026-10-20T05:00:00.000Z"],
      ["u1", "r1", "50cc", false, "2026-10-20T01:00:00.000Z"],
    ]);
  });
});

describe("transformHydrations", () => {
  it("calls an episode that starts near a boot full, and any other one new objects", () => {
    const bootTimes = replicaBootTimes(
      [transition("u1", "10:00:00", "online")],
      [replica({ id: "u1", createdAt: at("08:00:00") })],
    );

    const hydrations = transformHydrations(
      [
        episode("u1", "11:00:00", "11:00:30", 1),
        episode("u1", "10:01:30", "10:05:00"),
        episode("u1", "08:00:20", "08:04:00"),
      ],
      bootTimes,
    );

    expect(
      hydrations.map(({ startedAt, kind, durationMs }) => [
        startedAt.toISOString(),
        kind,
        durationMs,
      ]),
    ).toEqual([
      ["2026-10-20T08:00:20.000Z", "full", 220_000],
      ["2026-10-20T10:01:30.000Z", "full", 210_000],
      ["2026-10-20T11:00:00.000Z", "newObjects", 30_000],
    ]);
  });
});

describe("transformHydrations by object coverage", () => {
  it("calls an episode full when it covers every object the cluster had, even away from a boot", () => {
    const bootTimes = new Map([["u1", [at("08:00:00").getTime()]]]);
    // Eight objects throughout, a ninth created at 10:30.
    const objectCountAt = (ms: number) =>
      ms < at("10:30:00").getTime() ? 8 : 9;

    const hydrations = transformHydrations(
      [
        // A zero-downtime upgrade's rehydration, with no boot near it.
        episode("u1", "10:00:05", "10:06:00", 8),
        episode("u1", "10:30:01", "10:30:20", 1),
        episode("u1", "12:00:00", "12:00:01", 0),
      ],
      bootTimes,
      objectCountAt,
    );

    expect(hydrations.map(({ kind }) => kind)).toEqual([
      "full",
      "newObjects",
      "newObjects",
    ]);
  });
});

describe("findUnfinishedHydrations", () => {
  // Created inside the window, which bounds each replica's first attempt.
  const replicas = [replica({ id: "u1", createdAt: at("09:59:00") })];
  const find = ({
    transitions,
    oomKills = [],
    hydrations = [],
    replicaList = replicas,
    startMs = at("00:00:00").getTime(),
  }: {
    transitions: ReplicaTransition[];
    oomKills?: ReplicaTransition[];
    hydrations?: Hydration[];
    replicaList?: HydrationReplica[];
    startMs?: number;
  }) =>
    findUnfinishedHydrations({
      transitions,
      oomKills,
      hydrations,
      replicas: replicaList,
      bootTimes: replicaBootTimes(transitions, replicaList),
      startMs,
    }).map(({ startedAt, endedAt, reason }) => [
      startedAt?.toISOString(),
      endedAt.toISOString(),
      reason,
    ]);

  it("reports each out-of-memory restart of a replica that never finished hydrating", () => {
    const transitions = [
      transition("u1", "10:00:00", "online"),
      transition("u1", "10:05:00", "offline", "oom-killed"),
      transition("u1", "10:06:00", "online"),
      transition("u1", "10:21:00", "offline", "oom-killed"),
      transition("u1", "10:22:00", "online"),
    ];

    expect(find({ transitions })).toEqual([
      ["2026-10-20T10:00:00.000Z", "2026-10-20T10:05:00.000Z", "oom-killed"],
      ["2026-10-20T10:06:00.000Z", "2026-10-20T10:21:00.000Z", "oom-killed"],
    ]);
  });

  it("merges an OOM kill on another process into the same restart", () => {
    const transitions = [
      transition("u1", "10:00:00", "online"),
      transition("u1", "10:05:00", "offline"),
    ];
    const oomKills = [transition("u1", "10:05:20", "offline", "oom-killed")];

    expect(find({ transitions, oomKills })).toEqual([
      ["2026-10-20T10:00:00.000Z", "2026-10-20T10:05:00.000Z", "oom-killed"],
    ]);
  });

  it("skips a restart after the replica finished hydrating", () => {
    const transitions = [
      transition("u1", "10:00:00", "online"),
      transition("u1", "12:00:00", "offline", "oom-killed"),
    ];
    const bootTimes = replicaBootTimes(transitions, replicas);
    const hydrations = transformHydrations(
      [episode("u1", "10:00:10", "10:04:00")],
      bootTimes,
    );

    expect(find({ transitions, hydrations })).toEqual([]);
  });

  it("can't tell for an attempt that began before the window", () => {
    // Up since two days ago, which hydrated before the window starts and so
    // isn't among the hydrations read.
    const transitions = [
      transition("u1", "11:30:00", "offline", "oom-killed"),
      transition("u1", "11:31:00", "online"),
    ];

    expect(
      find({
        transitions,
        replicaList: [
          replica({ id: "u1", createdAt: new Date("2026-10-18T10:00:00Z") }),
        ],
        startMs: at("11:00:00").getTime(),
      }),
    ).toEqual([]);
  });

  it("doesn't take the online row of a zero-downtime upgrade for a boot", () => {
    // The upgrade rehydrated on new processes while the environment was
    // read-only, and its online row came when the new leader started.
    const transitions = [
      transition("u1", "10:16:30", "online"),
      transition("u1", "10:40:00", "offline", "oom-killed"),
    ];
    const hydrations = transformHydrations(
      [episode("u1", "10:00:05", "10:06:00")],
      replicaBootTimes(transitions, replicas),
    );

    expect(find({ transitions, hydrations })).toEqual([]);
  });

  it("counts a boot with no objects to hydrate as finished", () => {
    const transitions = [
      transition("u1", "10:00:00", "online"),
      transition("u1", "20:00:00", "offline", "oom-killed"),
    ];
    const bootTimes = replicaBootTimes(transitions, replicas);
    const sourceOnlyBoot = transformHydrations(
      [episode("u1", "10:00:01", "10:00:05", 0)],
      bootTimes,
    );

    expect(find({ transitions, hydrations: sourceOnlyBoot })).toEqual([]);
    expect(find({ transitions })).toHaveLength(1);
  });

  it("doesn't count the offline row a new pod records before it first comes online", () => {
    const transitions = [
      transition("u1", "10:00:00.300", "offline"),
      transition("u1", "10:01:30", "online"),
    ];
    const replicaList = [replica({ id: "u1", createdAt: at("10:00:00") })];
    const hydrations = transformHydrations(
      [episode("u1", "10:01:32", "10:06:00")],
      replicaBootTimes(transitions, replicaList),
    );

    expect(find({ transitions, replicaList })).toEqual([]);
    expect(find({ transitions, replicaList, hydrations })).toEqual([]);
  });

  it("skips a replica going offline when it's dropped, and restarts before the window", () => {
    const transitions = [
      transition("u1", "08:00:00", "online"),
      transition("u1", "08:05:00", "offline"),
      transition("u1", "10:00:00", "online"),
      transition("u1", "10:02:00", "offline"),
    ];

    expect(
      find({
        transitions,
        replicaList: [replica({ id: "u1", droppedAt: at("10:02:30") })],
        startMs: at("09:00:00").getTime(),
      }),
    ).toEqual([]);
  });
});

describe("typicalFullHydration", () => {
  it("takes the median of the latest full hydrations on replicas of the size", () => {
    const replicas = [
      replica({ id: "u1", size: "50cc", isCurrent: false }),
      replica({ id: "u2", size: "100cc" }),
    ];
    const bootTimes = new Map([
      ["u1", [at("01:00:00").getTime()]],
      [
        "u2",
        ["02:00:00", "03:00:00", "04:00:00"].map((time) => at(time).getTime()),
      ],
    ]);
    const hydrations = transformHydrations(
      [
        episode("u1", "01:00:00", "01:10:00"),
        episode("u2", "02:00:00", "02:04:00"),
        episode("u2", "03:00:00", "03:05:00"),
        episode("u2", "04:00:00", "04:03:00"),
        episode("u2", "05:00:00", "05:00:30", 1),
      ],
      bootTimes,
    );

    expect(typicalFullHydration(hydrations, replicas, "100cc")).toEqual({
      medianMs: 240_000,
      minMs: 180_000,
      maxMs: 300_000,
      count: 3,
    });
    expect(typicalFullHydration(hydrations, replicas, "25cc")).toBeUndefined();
  });
});

describe("heapLimitBytes", () => {
  it("uses the reported heap limit, else the size's memory plus disk", () => {
    const allocations = new Map([
      [
        "50cc",
        {
          processes: 1,
          workers: 1,
          cpuNanoCores: 1e9,
          memoryBytes: 800,
          diskBytes: 200,
        },
      ],
    ]);
    const current = replica({ id: "u2", size: "50cc" });
    const dropped = replica({ id: "u1", size: "50cc", isCurrent: false });

    expect(heapLimitBytes(current, new Map([["u2", 1500]]), allocations)).toBe(
      1500,
    );
    expect(heapLimitBytes(dropped, new Map(), allocations)).toBe(1000);
    expect(
      heapLimitBytes(replica({ id: "u5", size: "25cc" }), undefined, undefined),
    ).toBeUndefined();
  });
});

describe("transformObjectHydrationSpans", () => {
  it("splits each object's time into waiting for a slot and working, in start order", () => {
    const spans = transformObjectHydrationSpans(
      [
        {
          globalId: "u21",
          objectId: null,
          installedAt: at("10:00:00"),
          startedAt: at("10:00:38"),
          hydratedAt: at("10:01:20"),
        },
        {
          globalId: "u20",
          objectId: "u10",
          installedAt: at("10:00:00"),
          startedAt: null,
          hydratedAt: at("10:04:31"),
        },
      ],
      at("10:00:00"),
    );

    expect(spans).toEqual([
      {
        globalId: "u20",
        objectId: "u10",
        installedMs: 0,
        startedMs: 0,
        hydratedMs: 271_000,
        queuedMs: 0,
        workingMs: 271_000,
      },
      {
        globalId: "u21",
        objectId: null,
        installedMs: 0,
        startedMs: 38_000,
        hydratedMs: 80_000,
        queuedMs: 38_000,
        workingMs: 42_000,
      },
    ]);
    expect(summarizeObjectSpans(spans)).toEqual({
      slowest: spans[0],
      waitedCount: 1,
      maxQueuedMs: 38_000,
    });
  });
});
