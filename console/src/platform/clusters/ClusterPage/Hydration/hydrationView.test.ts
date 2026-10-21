// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  formatDate,
  FRIENDLY_DATETIME_FORMAT_NO_SECONDS,
} from "~/utils/dateFormat";

import { ReplicaStatus } from "../Configuration/clusterConfigurationModel";
import {
  Hydration,
  HydrationReplica,
  UnfinishedHydration,
} from "./hydrationModel";
import {
  hydrationVerdict,
  summarizeReplicasNow,
  toHydrationRows,
} from "./hydrationView";

const at = (time: string) => new Date(`2026-10-20T${time}Z`);

const replica = (id: string, name: string): HydrationReplica => ({
  id,
  name,
  size: "100cc",
  isCurrent: true,
  createdAt: undefined,
  droppedAt: undefined,
});

const hydration = (
  replicaId: string,
  startedAt: string,
  durationMs: number,
): Hydration => ({
  id: `${replicaId}|${startedAt}`,
  replicaId,
  startedAt: at(startedAt),
  finishedAt: new Date(at(startedAt).getTime() + durationMs),
  durationMs,
  objectCount: 8,
  peakHeapBytes: null,
  peakMemoryBytes: null,
  peakDiskBytes: null,
  kind: "full",
});

const restart = (
  replicaId: string,
  endedAt: string,
  reason: string | null = "oom-killed",
): UnfinishedHydration => ({
  id: `${replicaId}|${endedAt}`,
  replicaId,
  startedAt: undefined,
  endedAt: at(endedAt),
  reason,
});

const summarize = ({
  progress,
  hydrations = [],
  unfinished = [],
  bootTimes = new Map<string, number[]>(),
  createdObjects = [],
  isCreationTimesUnknown = false,
  statuses = {},
  clusterObjectCount = 9,
  nowMs = at("11:22:00").getTime(),
}: {
  /** Hydrated and total objects, and the ids not hydrated if they matter. */
  progress: Record<string, [number, number, string[]?]>;
  hydrations?: Hydration[];
  unfinished?: UnfinishedHydration[];
  bootTimes?: Map<string, number[]>;
  createdObjects?: Array<{ id: string; occurredAt: Date }>;
  isCreationTimesUnknown?: boolean;
  /** By replica id. A replica not listed is online. */
  statuses?: Record<string, ReplicaStatus>;
  clusterObjectCount?: number;
  nowMs?: number;
}) =>
  summarizeReplicasNow({
    replicas: [replica("u1", "r1"), replica("u2", "r2")],
    statusById: new Map(
      ["u1", "u2"].map((id) => [id, statuses[id] ?? { kind: "online" }]),
    ),
    clusterObjectCount,
    progressByReplicaId: new Map(
      Object.entries(progress).map(
        ([id, [hydratedCount, objectCount, unhydratedObjectIds]]) => [
          id,
          {
            hydratedCount,
            objectCount,
            unhydratedObjectIds:
              unhydratedObjectIds ??
              Array.from(
                { length: objectCount - hydratedCount },
                (_, index) => `u${100 + index}`,
              ),
          },
        ],
      ),
    ),
    bootTimes,
    hydrations,
    unfinished,
    createdObjects: isCreationTimesUnknown ? undefined : createdObjects,
    nowMs,
  });

describe("toHydrationRows", () => {
  it("orders finished hydrations by start and unfinished ones by end, newest first", () => {
    const rows = toHydrationRows(
      [
        hydration("u1", "09:00:00", 60_000),
        hydration("u1", "11:00:00", 60_000),
      ],
      [restart("u1", "10:00:00")],
    );

    expect(rows.map(({ type, id }) => [type, id])).toEqual([
      ["hydration", "u1|11:00:00"],
      ["unfinished", "u1|10:00:00"],
      ["hydration", "u1|09:00:00"],
    ]);
  });
});

describe("summarizeReplicasNow", () => {
  it("counts only restarts since a replica last finished hydrating", () => {
    const replicasNow = summarize({
      progress: { u1: [3, 9], u2: [9, 9] },
      hydrations: [hydration("u1", "09:00:00", 60_000)],
      unfinished: [restart("u1", "08:00:00"), restart("u1", "10:00:00")],
    });

    expect(
      replicasNow.map(({ replica: { id }, state, restarts }) => [
        id,
        state,
        restarts.map(({ id: restartId }) => restartId),
      ]),
    ).toEqual([
      ["u1", "restarting", ["u1|10:00:00"]],
      ["u2", "hydrated", []],
    ]);
  });
});

describe("hydrationVerdict", () => {
  const NOW_MS = at("11:22:00").getTime();

  it("calls out replicas that keep restarting before they finish", () => {
    const verdict = hydrationVerdict(
      summarize({
        progress: { u1: [3, 10], u2: [2, 10] },
        unfinished: [
          restart("u1", "10:31:00"),
          restart("u2", "10:33:00"),
          restart("u1", "10:47:00"),
        ],
      }),
      NOW_MS,
    );

    expect(verdict).toEqual({
      variant: "error",
      label: `r1 and r2 haven't finished hydrating since ${formatDate(at("10:31:00"), FRIENDLY_DATETIME_FORMAT_NO_SECONDS)}.`,
      message:
        "Between them, they restarted 3 times, each after running out of memory.",
      isFailing: true,
    });
  });

  it("says how far a hydrating replica has got and how long it last took", () => {
    const verdict = hydrationVerdict(
      summarize({
        progress: { u1: [9, 9], u2: [5, 9] },
        hydrations: [hydration("u2", "09:00:00", 280_000)],
        bootTimes: new Map([["u2", [at("11:19:50").getTime()]]]),
      }),
      NOW_MS,
    );

    expect(verdict).toMatchObject({
      variant: "warning",
      label: "r2 is hydrating: 5 of 9 objects, 2m 10s so far.",
      message: "Its last full hydration took 4m 40s.",
    });
  });

  it("dates a hydration of new objects from the first one's creation, not the replica's boot", () => {
    const verdict = hydrationVerdict(
      summarize({
        progress: { u1: [8, 9, ["u10"]], u2: [9, 9] },
        hydrations: [hydration("u1", "08:00:00", 280_000)],
        // A boot after the last hydration finished, as a zero-downtime
        // upgrade records.
        bootTimes: new Map([["u1", [at("08:20:00").getTime()]]]),
        createdObjects: [{ id: "u10", occurredAt: at("11:20:30") }],
      }),
      NOW_MS,
    );

    expect(verdict?.label).toBe(
      "r1 is hydrating: 8 of 9 objects, 1m 30s so far.",
    );
  });

  it("dates a hydration from the boot when an object from before it isn't hydrated", () => {
    const [r1] = summarize({
      progress: { u1: [7, 9, ["u10", "u11"]] },
      bootTimes: new Map([["u1", [at("11:19:50").getTime()]]]),
      createdObjects: [{ id: "u10", occurredAt: at("11:21:00") }],
    });

    expect(r1.startedAt).toEqual(at("11:19:50"));
  });

  it("leaves out how long a replica has been hydrating until creation times load", () => {
    const verdict = hydrationVerdict(
      summarize({
        progress: { u1: [9, 9], u2: [5, 9] },
        bootTimes: new Map([["u2", [at("11:19:50").getTime()]]]),
        isCreationTimesUnknown: true,
      }),
      NOW_MS,
    );

    expect(verdict?.label).toBe("r2 is hydrating: 5 of 9 objects.");
  });

  it("has nothing to say once every replica is hydrated", () => {
    expect(
      hydrationVerdict(
        summarize({
          progress: { u1: [9, 9], u2: [9, 9] },
          hydrations: [hydration("u2", "10:00:00", 271_000)],
        }),
        NOW_MS,
      ),
    ).toBeUndefined();
  });

  it("has nothing to say for a cluster with nothing to hydrate", () => {
    expect(
      hydrationVerdict(
        summarize({
          progress: { u1: [0, 0], u2: [0, 0] },
          clusterObjectCount: 0,
        }),
        NOW_MS,
      ),
    ).toBeUndefined();
  });

  it("calls out an offline replica rather than its last progress", () => {
    const replicasNow = summarize({
      progress: { u1: [9, 9], u2: [9, 9] },
      statuses: { u2: { kind: "offline", reason: null } },
    });

    expect(replicasNow.map(({ state }) => state)).toEqual([
      "hydrated",
      "offline",
    ]);
    expect(hydrationVerdict(replicasNow, NOW_MS)).toMatchObject({
      variant: "warning",
      label: "r2 is offline.",
    });
  });

  it("stops calling a replica starting once it has been up a while without reporting", () => {
    // As for a cluster with introspection turned off, whose replicas never
    // report.
    const replicasNow = summarize({
      progress: { u1: [0, 0], u2: [0, 0] },
      bootTimes: new Map([
        ["u1", [at("10:00:00").getTime()]],
        ["u2", [at("10:00:00").getTime()]],
      ]),
    });

    expect(replicasNow.map(({ state }) => state)).toEqual([
      "hydrated",
      "hydrated",
    ]);
    expect(hydrationVerdict(replicasNow, NOW_MS)).toBeUndefined();
  });

  it("calls a replica that hasn't reported starting, on a cluster with objects", () => {
    const replicasNow = summarize({
      progress: { u1: [9, 9], u2: [0, 0] },
      bootTimes: new Map([["u2", [at("11:21:00").getTime()]]]),
    });

    expect(replicasNow.map(({ state }) => state)).toEqual([
      "hydrated",
      "starting",
    ]);
    expect(hydrationVerdict(replicasNow, NOW_MS)).toMatchObject({
      variant: "warning",
      label: "r2 is starting.",
    });
  });

  it("explains a cluster with no replicas", () => {
    expect(hydrationVerdict([], NOW_MS)).toMatchObject({
      variant: "info",
      label: "This cluster has no replicas, so nothing on it is hydrated.",
    });
  });
});
