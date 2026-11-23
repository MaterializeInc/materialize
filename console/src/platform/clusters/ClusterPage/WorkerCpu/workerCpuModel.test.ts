// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ProcessMetricsSample } from "~/api/materialize/cluster/replicaProcessMetrics";
import { DatabaseObject } from "~/api/materialize/objects";

import {
  accumulateRowElapsedNs,
  accumulateWorkerElapsedNs,
  ADHOC_ROW_ID,
  cpuVerdict,
  dataflowCounters,
  DataflowExport,
  dataflowRowIds,
  DataflowWorkerCpu,
  hasCounterReset,
  hasDataflowCounterReset,
  largestShare,
  lifetimeWorkerSkew,
  markedWorkers,
  measureLatest,
  Measurement,
  ObjectCpuRead,
  objectCpuRows,
  objectSpread,
  ONE_WORKER_RATIO,
  ownDataflows,
  processOfWorker,
  replicaCpuPoints,
  runsStorageDataflow,
  samplesInMeasurement,
  SKEW_RATIO,
  SYSTEM_ROW_ID,
  topObjectOnWorker,
  topObjectsOnWorker,
  verdictObjectId,
  workerBusyBetween,
  workerBusySeries,
  WorkerCpuReading,
  workerLabel,
  WorkerReplica,
  workersPerCore,
  workerVerdict,
} from "./workerCpuModel";

const SECOND_NS = 1e9;

const reading = (atS: number, elapsedS: number[]): WorkerCpuReading => ({
  atMs: atS * 1000,
  elapsedNsByWorker: new Map(elapsedS.map((s, w) => [w, s * SECOND_NS])),
});

const cpu = (dataflowId: string, perWorkerS: number[]): DataflowWorkerCpu[] =>
  perWorkerS.map((s, workerId) => ({
    dataflowId,
    workerId,
    elapsedNs: s * SECOND_NS,
  }));

const busy = (fractions: number[]) =>
  workerBusyBetween(
    reading(
      0,
      fractions.map(() => 0),
    ),
    reading(
      10,
      fractions.map((f) => f * 10),
    ),
  );

describe("workerBusyBetween", () => {
  it("gives each worker's share of one core between two readings, busiest first", () => {
    expect(
      workerBusyBetween(reading(0, [100, 50, 7]), reading(10, [109, 51, 8])),
    ).toEqual([
      { workerId: 0, busyFraction: 0.9 },
      { workerId: 1, busyFraction: 0.1 },
      { workerId: 2, busyFraction: 0.1 },
    ]);
  });

  it("counts a counter that went backwards, as after a restart, as idle", () => {
    expect(
      workerBusyBetween(reading(0, [500]), reading(10, [3]))[0].busyFraction,
    ).toEqual(0);
  });

  it("caps a worker at one core when reads land unevenly", () => {
    expect(
      workerBusyBetween(reading(0, [0]), reading(10, [10.4]))[0].busyFraction,
    ).toEqual(1);
  });
});

describe("workerBusySeries", () => {
  it("names the busiest worker and the average between each pair of readings", () => {
    expect(
      workerBusySeries(
        [reading(0, [0, 0]), reading(5, [4, 1]), reading(10, [5, 5])],
        0,
      ),
    ).toEqual([
      {
        atMs: 5000,
        busiest: { workerId: 0, busyFraction: 0.8 },
        averageBusyFraction: 0.5,
      },
      {
        atMs: 10000,
        busiest: { workerId: 1, busyFraction: 0.8 },
        averageBusyFraction: 0.5,
      },
    ]);
  });

  it("leaves out the span of a pause and marks where the line resumes", () => {
    expect(
      workerBusySeries(
        [
          reading(0, [0]),
          reading(5, [4]),
          { ...reading(600, [100]), afterGap: true },
          reading(605, [101]),
        ],
        0,
      ).map(({ atMs, afterGap }) => [atMs, Boolean(afterGap)]),
    ).toEqual([
      [5000, false],
      [605000, true],
    ]);
  });

  it("averages each point over the trailing window, smoothing one-off ticks", () => {
    // Worker 0 is steadily at 50%, but one 5s read caught an extra tick.
    const readings = [0, 5, 10, 15, 20, 25, 30, 35].map((atS) =>
      reading(atS, [atS * 0.5 + (atS >= 20 ? 1 : 0)]),
    );

    expect(
      workerBusySeries(readings, 30_000).map(({ atMs, busiest }) => [
        atMs / 1000,
        Math.round(busiest.busyFraction * 100),
      ]),
    ).toEqual([
      [10, 50],
      [15, 50],
      [20, 55],
      [25, 54],
      [30, 53],
      [35, 53],
    ]);
  });
});

describe("counter resets", () => {
  it("flags a reading where any worker's counter fell", () => {
    expect(
      hasCounterReset(
        reading(0, [100, 100]).elapsedNsByWorker,
        reading(5, [104, 40]).elapsedNsByWorker,
      ),
    ).toBe(true);
    expect(
      hasCounterReset(
        reading(0, [100, 100]).elapsedNsByWorker,
        reading(5, [104, 103]).elapsedNsByWorker,
      ),
    ).toBe(false);
  });

  it("flags a per-dataflow read where any dataflow's counter fell, as after a restart", () => {
    const row = (dataflowId: string, elapsedNs: number) => ({
      dataflowId,
      workerId: 0,
      elapsedNs,
    });
    expect(hasDataflowCounterReset([row("5", 900)], [row("5", 20)])).toBe(true);
    expect(
      hasDataflowCounterReset([row("5", 900)], [row("5", 950), row("6", 1)]),
    ).toBe(false);
  });

  // One worker whose maintained dataflows add 0.5s every 5s, while ad-hoc
  // queries hold 4s of time at even readings and 1s at odd ones, as each
  // query's dataflow finishes and the next starts. Every odd reading is below
  // the one before it.
  const adHocLoad = (seconds: number) =>
    Array.from({ length: seconds / 5 + 1 }, (_, index) =>
      reading(index * 5, [index * 0.5 + (index % 2 === 0 ? 4 : 1)]),
    );

  it("keeps the line through ad-hoc queries' dataflows finishing between readings", () => {
    const points = workerBusySeries(adHocLoad(60));

    expect(points.map(({ atMs }) => atMs / 1000)).toEqual([
      10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60,
    ]);
    expect(points.some(({ afterGap }) => afterGap)).toBe(false);
  });

  it("breaks the line after a restart instead of drawing across it", () => {
    const restart = [0, 5, 10, 15, 20, 25, 30, 35].map((atS) =>
      reading(atS, [atS < 15 ? 100 + atS : atS - 14]),
    );

    expect(
      workerBusySeries(restart, 10_000).map(({ atMs, afterGap }) => [
        atMs / 1000,
        Boolean(afterGap),
      ]),
    ).toEqual([
      [10, false],
      [25, true],
      [30, false],
      [35, false],
    ]);
  });
});

describe("accumulateWorkerElapsedNs", () => {
  const counter = (dataflowId: string, workerId: number, seconds: number) => ({
    dataflowId,
    workerId,
    elapsedNs: seconds * SECOND_NS,
  });
  const totalsS = (totals: Map<number, number>) =>
    [...totals].map(([workerId, ns]) => [workerId, ns / SECOND_NS]);
  const previousOf = (
    rows: DataflowWorkerCpu[],
    elapsedS: Array<[number, number]>,
  ) => ({
    elapsedNsByWorker: new Map(
      elapsedS.map(([workerId, s]) => [workerId, s * SECOND_NS]),
    ),
    counters: dataflowCounters(rows),
  });

  it("sums the running dataflows' lifetimes without a previous reading", () => {
    expect(
      totalsS(
        accumulateWorkerElapsedNs([
          counter("1", 0, 10),
          counter("2", 0, 5),
          counter("1", 1, 4),
        ]),
      ),
    ).toEqual([
      [0, 15],
      [1, 4],
    ]);
  });

  it("carries the totals on by each dataflow's growth, so a dropped dataflow doesn't make them fall", () => {
    const before = [counter("1", 0, 10), counter("2", 0, 50)];
    const previous = previousOf(before, [[0, 60]]);

    expect(
      totalsS(accumulateWorkerElapsedNs([counter("1", 0, 12)], previous)),
    ).toEqual([[0, 62]]);
  });

  it("counts a dataflow that started since the previous reading in full", () => {
    const previous = previousOf([counter("1", 0, 10)], [[0, 10]]);

    expect(
      totalsS(
        accumulateWorkerElapsedNs(
          [counter("1", 0, 11), counter("7", 0, 3)],
          previous,
        ),
      ),
    ).toEqual([[0, 14]]);
  });

  it("counts a dataflow whose counter fell, as under an id reused after a restart, as new", () => {
    const previous = previousOf([counter("1", 0, 100)], [[0, 100]]);

    expect(
      totalsS(accumulateWorkerElapsedNs([counter("1", 0, 2)], previous)),
    ).toEqual([[0, 102]]);
  });
});

describe("dataflowRowIds", () => {
  const running = (...dataflowIds: string[]) =>
    dataflowIds.flatMap((dataflowId) => cpu(dataflowId, [1]));
  const sessionExport = (dataflowId: string): DataflowExport => ({
    dataflowId,
    objectId: `t${dataflowId}`,
    dataflowName: `Dataflow: oneshot-select-t${dataflowId}`,
  });

  it("keeps a running dataflow's row once it exports nothing, and forgets a dataflow that stopped", () => {
    const remembered = dataflowRowIds(running("7", "8"), [
      sessionExport("7"),
      { dataflowId: "8", objectId: "u8" },
    ]);

    expect([...dataflowRowIds(running("7"), [], remembered)]).toEqual([
      ["7", ADHOC_ROW_ID],
    ]);
  });

  it("takes a dataflow's row from its current export over a remembered one", () => {
    const remembered = new Map([["7", "u1"]]);

    expect([
      ...dataflowRowIds(
        running("7"),
        [{ dataflowId: "7", objectId: "u2" }],
        remembered,
      ),
    ]).toEqual([["7", "u2"]]);
  });

  it("gives no row to a transient dataflow whose name isn't known", () => {
    expect(
      dataflowRowIds(running("7"), [{ dataflowId: "7", objectId: "t7" }]).size,
    ).toBe(0);
  });
});

describe("accumulateRowElapsedNs", () => {
  const rowIds = new Map([
    ["1", "u1"],
    ["9", ADHOC_ROW_ID],
  ]);
  const totalsS = (totals: Map<string, number[]>) =>
    [...totals].map(([rowId, ns]) => [rowId, ns.map((n) => n / SECOND_NS)]);

  it("carries each row's totals on by its dataflows' growth, so a finished dataflow keeps its time", () => {
    const previous = {
      counters: dataflowCounters([...cpu("1", [10, 0]), ...cpu("9", [5, 0])]),
      rowTotals: new Map([
        ["u1", [10 * SECOND_NS, 0]],
        [ADHOC_ROW_ID, [5 * SECOND_NS, 0]],
      ]),
    };

    // Dataflow 9 finished since.
    const { rowTotals, grown } = accumulateRowElapsedNs({
      rows: cpu("1", [12, 0]),
      rowIds,
      workerCount: 2,
      previous,
    });

    expect(totalsS(rowTotals)).toEqual([
      ["u1", [12, 0]],
      [ADHOC_ROW_ID, [5, 0]],
    ]);
    expect([...grown]).toEqual([["u1", new Set(["1"])]]);
  });

  it("counts a dataflow new since the previous reading, or under a reused id, in full", () => {
    const previous = {
      counters: dataflowCounters(cpu("1", [100, 100])),
      rowTotals: new Map([["u1", [100 * SECOND_NS, 100 * SECOND_NS]]]),
    };

    const { rowTotals } = accumulateRowElapsedNs({
      rows: [...cpu("1", [2, 1]), ...cpu("9", [3, 0])],
      rowIds,
      workerCount: 2,
      previous,
    });

    expect(totalsS(rowTotals)).toEqual([
      ["u1", [102, 101]],
      [ADHOC_ROW_ID, [3, 0]],
    ]);
  });

  it("leaves out workers past the worker count and dataflows without a row", () => {
    const { rowTotals } = accumulateRowElapsedNs({
      rows: [...cpu("1", [4, 6]), ...cpu("5", [9, 9])],
      rowIds,
      workerCount: 1,
    });

    expect(totalsS(rowTotals)).toEqual([["u1", [4]]]);
  });
});

describe("ownDataflows", () => {
  const running = (...dataflowIds: string[]) =>
    dataflowIds.map((dataflowId) => ({
      dataflowId,
      workerId: 0,
      elapsedNs: 1,
    }));
  const exported = (
    objectId: string,
    dataflowId: string,
    connectionId: number | null = null,
  ) => ({ objectId, dataflowId, connectionId, dataflowName: null });

  it("finds the dataflows exported under the session's connection", () => {
    const own = ownDataflows({
      dataflows: running("5", "6"),
      exports: [exported("u1", "5"), exported("t9", "6", 42)],
      connectionId: 42,
      remembered: [],
    });

    expect([...own.dataflowIds]).toEqual(["6"]);
    expect(own.remembered).toEqual([{ objectId: "t9", dataflowId: "6" }]);
  });

  it("keeps an earlier session's dataflow while it runs after its tag and export leave", () => {
    const remembered = [{ objectId: "t9", dataflowId: "6" }];

    const untagged = ownDataflows({
      dataflows: running("6", "8"),
      exports: [exported("t9", "6"), exported("t11", "8", 43)],
      connectionId: 43,
      remembered,
    });
    const unexported = ownDataflows({
      dataflows: running("6", "8"),
      exports: [exported("t11", "8", 43)],
      connectionId: 43,
      remembered,
    });

    expect([...untagged.dataflowIds].sort()).toEqual(["6", "8"]);
    expect([...unexported.dataflowIds].sort()).toEqual(["6", "8"]);
  });

  it("forgets an earlier session's dataflow once it stops", () => {
    const own = ownDataflows({
      dataflows: running("8"),
      exports: [exported("t11", "8", 43)],
      connectionId: 43,
      remembered: [{ objectId: "t9", dataflowId: "6" }],
    });

    expect(own.remembered).toEqual([{ objectId: "t11", dataflowId: "8" }]);
  });

  it("leaves a remembered id to the object that reuses it, as after a restart", () => {
    const own = ownDataflows({
      dataflows: running("6"),
      exports: [exported("u3", "6")],
      connectionId: 43,
      remembered: [{ objectId: "t9", dataflowId: "6" }],
    });

    expect(own.dataflowIds.size).toBe(0);
    expect(own.remembered).toEqual([]);
  });
});

describe("workerVerdict", () => {
  it("calls one worker at its core's limit saturated when the rest have room", () => {
    expect(workerVerdict(busy([0.97, 0.14, 0.12, 0.13])).kind).toEqual(
      "saturated",
    );
  });

  it("catches a pegged worker on a two-worker replica", () => {
    expect(workerVerdict(busy([1, 0.2])).kind).toEqual("saturated");
  });

  it("catches a pegged worker whose peers each run well behind it", () => {
    expect(workerVerdict(busy([1, 0.4, 0.4, 0.4])).kind).toEqual("saturated");
  });

  it("calls an evenly busy replica short on CPU, not skewed", () => {
    expect(workerVerdict(busy([0.98, 0.9, 0.88, 0.92])).kind).toEqual(
      "allBusy",
    );
  });

  it("calls a worker at twice the average uneven when it still has room", () => {
    expect(workerVerdict(busy([0.5, 0.1, 0.1, 0.1])).kind).toEqual("uneven");
  });

  it("calls a lead over the average light while no worker nears its limit", () => {
    expect(workerVerdict(busy([0.08, 0.02, 0.02, 0.02])).kind).toEqual("light");
  });

  it("calls workers within twice the average balanced", () => {
    expect(workerVerdict(busy([0.21, 0.17, 0.16, 0.18])).kind).toEqual(
      "balanced",
    );
  });

  it("calls a replica with almost no dataflow work idle, whatever the ratios", () => {
    expect(workerVerdict(busy([0.02, 0, 0, 0])).kind).toEqual("idle");
  });

  it("calls a worker at its core's limit pegged when it leads the others by less than SKEW_RATIO", () => {
    expect(workerVerdict(busy([1, 0.55, 0.55, 0.55])).kind).toEqual("pegged");
  });

  it("calls workers as busy as a worker at its limit balanced, keeping it among those at their limit", () => {
    const verdict = workerVerdict(busy([0.92, 0.8, 0.8, 0.8]));

    expect(verdict.kind).toBe("balanced");
    expect(
      verdict.kind !== "idle" &&
        verdict.atLimit.map(({ workerId }) => workerId),
    ).toEqual([0]);
  });

  it("keeps every worker at its limit on a busy replica, busiest first", () => {
    const verdict = workerVerdict(busy([0.97, 0.96, 0.95, 0.98, 0.7]));

    expect(verdict.kind).toBe("allBusy");
    expect(
      verdict.kind !== "idle" &&
        verdict.atLimit.map(({ workerId }) => workerId),
    ).toEqual([3, 0, 1, 2]);
  });
});

describe("cpuVerdict", () => {
  it("leads with a busy replica's CPU when its workers share the work evenly", () => {
    const verdict = cpuVerdict(busy([0.58, 0.55, 0.54, 0.53]), 0.89, 1);
    expect(verdict).toMatchObject({
      kind: "highCpu",
      replicaCpuFraction: 0.89,
      dataflowFraction: 0.55,
    });
    expect(verdict.kind === "highCpu" && verdict.outsideFraction).toBeCloseTo(
      0.34,
    );
  });

  it("measures dataflow time against cores when a size runs fewer workers than cores", () => {
    expect(
      cpuVerdict(
        busy([0.5, 0.5, 0.5, 0.5]),
        0.8,
        workersPerCore({
          processes: 1,
          workers: 4,
          cpuNanoCores: 16e9,
          memoryBytes: 0,
          diskBytes: 0,
        }),
      ),
    ).toMatchObject({ kind: "highCpu", dataflowFraction: 0.125 });
  });

  it("explains a busy replica's CPU even when its dataflows are idle", () => {
    expect(cpuVerdict(busy([0.004, 0, 0, 0]), 0.8, 1)).toMatchObject({
      kind: "highCpu",
      dataflowFraction: 0.001,
      outsideFraction: 0.799,
    });
  });

  it("leads with a busy replica's CPU over uneven work, keeping the uneven worker", () => {
    expect(cpuVerdict(busy([0.7, 0.2, 0.2, 0.2]), 0.9, 1)).toMatchObject({
      kind: "highCpu",
      uneven: { busiest: { workerId: 0, busyFraction: 0.7 } },
    });
  });

  it("reports a pegged worker on a busy two-worker replica as saturated, not high CPU", () => {
    expect(cpuVerdict(busy([1, 0.2]), 0.8, 1).kind).toEqual("saturated");
  });

  it("reports a pegged worker on a busy replica as pegged, not high CPU", () => {
    expect(cpuVerdict(busy([1, 0.55, 0.55, 0.55]), 0.8, 1).kind).toEqual(
      "pegged",
    );
  });

  it("keeps the workers at their limit on a high CPU verdict", () => {
    const verdict = cpuVerdict(busy([0.95, 0.96, 0.94, 0.8]), 0.95, 1);

    expect(verdict.kind).toBe("highCpu");
    expect(
      verdict.kind === "highCpu" &&
        verdict.atLimit.map(({ workerId }) => workerId),
    ).toEqual([1, 0, 2]);
  });

  it("reports a saturated worker even on a busy replica", () => {
    expect(cpuVerdict(busy([0.97, 0.14, 0.12, 0.13]), 0.9, 1).kind).toEqual(
      "saturated",
    );
  });

  it("judges the workers alone when replica CPU is low or unknown", () => {
    expect(cpuVerdict(busy([0.21, 0.17, 0.16, 0.18]), 0.3, 1).kind).toEqual(
      "balanced",
    );
    expect(
      cpuVerdict(busy([0.21, 0.17, 0.16, 0.18]), undefined, 1).kind,
    ).toEqual("balanced");
  });
});

describe("lifetimeWorkerSkew", () => {
  it("compares the busiest worker's lifetime time to the other workers' average", () => {
    expect(lifetimeWorkerSkew(reading(0, [70, 10, 10, 10]))).toEqual({
      workerId: 0,
      ratio: 7,
    });
    expect(lifetimeWorkerSkew(reading(0, [100, 20]))?.ratio).toEqual(5);
  });
});

describe("objectCpuRows", () => {
  const exports = [
    { dataflowId: "1", objectId: "u10" },
    { dataflowId: "2", objectId: "u20" },
  ];

  it("measures each object's hottest worker against the other workers' average", () => {
    const { rows } = objectCpuRows({
      latest: [
        ...cpu("1", [70, 10, 10, 10]),
        ...cpu("2", [300, 300, 300, 300]),
      ],
      exports,
      workerCount: 4,
    });

    expect(
      [...rows]
        .sort((a, b) => a.objectId.localeCompare(b.objectId))
        .map(({ objectId, hottestWorkerId, ratio }) => [
          objectId,
          hottestWorkerId,
          ratio,
        ]),
    ).toEqual([
      ["u10", 0, 7],
      ["u20", 0, 1],
    ]);
  });

  it("calls an object on one worker of two skewed, not capped at twice an even share", () => {
    const { rows } = objectCpuRows({
      latest: cpu("1", [100, 20]),
      exports,
      workerCount: 2,
    });

    expect(rows[0].ratio).toEqual(5);
  });

  it("pools the temporary dataflows of sessions' queries, subscribes and writes into one row", () => {
    const { rows } = objectCpuRows({
      latest: [...cpu("7", [3, 1]), ...cpu("8", [1, 1]), ...cpu("9", [2, 0])],
      exports: [
        {
          dataflowId: "7",
          objectId: "t41",
          dataflowName: "Dataflow: oneshot-select-t40",
        },
        {
          dataflowId: "8",
          objectId: "t42",
          dataflowName: "Dataflow: subscribe-t42",
        },
        {
          dataflowId: "9",
          objectId: "t43",
          dataflowName: "Dataflow: frontend-read-then-write-subscribe-t43",
        },
      ],
      workerCount: 2,
    });

    expect(
      rows.map(({ objectId, dataflowCount, perWorkerNs }) => [
        objectId,
        dataflowCount,
        perWorkerNs.map((ns) => ns / SECOND_NS),
      ]),
    ).toEqual([[ADHOC_ROW_ID, 3, [6, 2]]]);
  });

  it("pools the coordinator's own transient dataflows into one row that's never the object to start with", () => {
    const { rows, workerTotalsNs } = objectCpuRows({
      latest: [
        ...cpu("1", [2, 1]),
        ...cpu("3", [5, 0]),
        ...cpu("4", [3, 0]),
        ...cpu("9", [1, 1]),
      ],
      exports: [
        ...exports,
        {
          dataflowId: "3",
          objectId: "t30",
          dataflowName: "Dataflow: introspection-subscribe-t30",
        },
        {
          dataflowId: "4",
          objectId: "t31",
          dataflowName: "Dataflow: metric-sink-mz_metric_arrangement_sizes-u1",
        },
        { dataflowId: "9", objectId: "t32" },
      ],
      workerCount: 2,
    });

    expect(
      rows.map(({ objectId, dataflowCount, perWorkerNs }) => [
        objectId,
        dataflowCount,
        perWorkerNs.map((ns) => ns / SECOND_NS),
      ]),
    ).toEqual([
      ["u10", 1, [2, 1]],
      [SYSTEM_ROW_ID, 2, [8, 0]],
    ]);
    expect(workerTotalsNs.map((ns) => ns / SECOND_NS)).toEqual([11, 2]);
    expect(largestShare(rows, workerTotalsNs)?.objectId).toBe("u10");
    expect(topObjectOnWorker(rows, workerTotalsNs, 0)?.objectId).toBe("u10");
  });

  it("counts the queries row like an object, as the per-worker ranking does, but not Materialize's own dataflows", () => {
    const row = (objectId: string, totalNs: number) => ({
      objectId,
      dataflowCount: 1,
      perWorkerNs: [totalNs],
      totalNs,
      hottestWorkerId: 0,
      ratio: 1,
    });
    const rows = [
      row(SYSTEM_ROW_ID, 50),
      row(ADHOC_ROW_ID, 30),
      row("u10", 20),
    ];

    expect(largestShare(rows, [100])).toEqual({
      objectId: ADHOC_ROW_ID,
      share: 0.3,
    });
    expect(topObjectOnWorker(rows, [100], 0)?.objectId).toBe(ADHOC_ROW_ID);
  });

  it("names the object with the most of the replica's dataflow time, with its share", () => {
    const { rows, workerTotalsNs } = objectCpuRows({
      latest: [
        ...cpu("1", [40, 40]),
        ...cpu("2", [35, 35]),
        ...cpu("99", [25, 25]),
      ],
      exports,
      workerCount: 2,
    });

    expect(largestShare(rows, workerTotalsNs)).toEqual({
      objectId: "u10",
      share: 0.4,
    });
  });

  it("counts a dataflow that exports nothing in each worker's total without a row", () => {
    const { rows, workerTotalsNs } = objectCpuRows({
      latest: [...cpu("1", [6, 2]), ...cpu("99", [4, 0])],
      exports,
      workerCount: 2,
    });

    expect(rows.map((row) => row.objectId)).toEqual(["u10"]);
    expect(workerTotalsNs.map((ns) => ns / SECOND_NS)).toEqual([10, 2]);
    expect(topObjectOnWorker(rows, workerTotalsNs, 0)).toMatchObject({
      objectId: "u10",
      share: 0.6,
    });
  });

  it("lists a worker's objects by its time on them, against the other workers", () => {
    const { rows, workerTotalsNs } = objectCpuRows({
      latest: [...cpu("1", [12, 2, 2]), ...cpu("2", [4, 4, 4])],
      exports,
      workerCount: 3,
    });

    expect(
      topObjectsOnWorker(rows, workerTotalsNs, 0, 3).map(
        ({ objectId, ns, share, othersAverageNs }) => [
          objectId,
          ns / SECOND_NS,
          share,
          othersAverageNs / SECOND_NS,
        ],
      ),
    ).toEqual([
      ["u10", 12, 0.75, 2],
      ["u20", 4, 0.25, 4],
    ]);
  });
});

describe("measureLatest", () => {
  const u1 = [{ dataflowId: "1", objectId: "u1" }];
  // A read whose rows' totals are the dataflows' lifetimes, as from a
  // sampler's first reading.
  const read = (
    atS: number,
    rows: DataflowWorkerCpu[],
    exports: DataflowExport[] = u1,
  ): ObjectCpuRead => {
    const { rowTotals, grown } = accumulateRowElapsedNs({
      rows,
      rowIds: dataflowRowIds(rows, exports),
      workerCount: 2,
    });
    return {
      atMs: atS * 1000,
      rows,
      exports,
      rowTotals: [...rowTotals],
      grownDataflows: [...grown].map(([rowId, ids]) => [rowId, [...ids]]),
    };
  };
  const adhocExport = (dataflowId: string): DataflowExport => ({
    dataflowId,
    objectId: `t${dataflowId}`,
    dataflowName: `Dataflow: oneshot-select-t${dataflowId}`,
  });
  // u1's dataflow takes half of worker 0 and a quarter of worker 1.
  const steadyS = (atS: number) => [10 + 0.5 * atS, 10 + 0.25 * atS];
  const steadyReads = (readsAtS: number[]) =>
    readsAtS.map((atS) => read(atS, cpu("1", steadyS(atS))));
  const steadyReadings = (readingsAtS: number[]) =>
    readingsAtS.map((atS) => reading(atS, steadyS(atS)));
  const measureSteady = (readsAtS: number[], startsFromMs?: number) =>
    measureLatest({
      reads: steadyReads(readsAtS),
      readings: steadyReadings(readsAtS),
      workerCount: 2,
      startsFromMs,
    });
  const spanS = (measurement: Measurement | undefined) =>
    measurement && [measurement.startMs / 1000, measurement.endMs / 1000];
  const inSeconds = (ns: number[] | undefined) => ns?.map((n) => n / SECOND_NS);

  it("measures from the latest read a minute to 100 seconds before the last one", () => {
    const measurement = measureSteady([0, 30, 45, 60, 90]);

    expect(spanS(measurement)).toEqual([30, 90]);
    expect(measurement?.workers).toEqual([
      { workerId: 0, busyFraction: 0.5 },
      { workerId: 1, busyFraction: 0.25 },
    ]);
    expect(inSeconds(measurement?.workerTotalsNs)).toEqual([30, 15]);
    expect(
      measurement?.rows.map(({ objectId, perWorkerNs }) => [
        objectId,
        inSeconds(perWorkerNs),
      ]),
    ).toEqual([["u1", [30, 15]]]);
  });

  it("keeps an object's time within its worker's when a tick lands just inside the span", () => {
    // u1 grew 61 s on worker 0 over a 60 s span.
    const measurement = measureLatest({
      reads: [read(0, cpu("1", [0, 0])), read(60, cpu("1", [61, 10]))],
      readings: [reading(0, [0, 0]), reading(60, [61, 10])],
      workerCount: 2,
    });

    expect(measurement?.workers[0]).toEqual({ workerId: 0, busyFraction: 1 });
    expect(inSeconds(measurement?.workerTotalsNs)).toEqual([61, 10]);
    expect(
      topObjectOnWorker(measurement!.rows, measurement!.workerTotalsNs, 0)
        ?.share,
    ).toBe(1);
  });

  it("measures spans of exactly a minute and exactly 3 minutes", () => {
    expect(spanS(measureSteady([0, 60]))).toEqual([0, 60]);
    expect(spanS(measureSteady([0, 180]))).toEqual([0, 180]);
  });

  it("measures nothing with under a minute of reads, or only reads over 3 minutes back", () => {
    expect(measureSteady([0, 30, 59])).toBeUndefined();
    expect(measureSteady([0, 181])).toBeUndefined();
    expect(measureSteady([0, 190, 200])).toBeUndefined();
  });

  it("measures a longer span when the counters advance in steps too long for a read a minute back", () => {
    expect(spanS(measureSteady([0, 52, 104]))).toEqual([0, 104]);
  });

  it("doesn't measure across a pause", () => {
    const readings = steadyReadings([0, 30, 60, 90]);
    readings[2] = { ...readings[2], afterGap: true };

    expect(
      spanS(
        measureLatest({
          reads: steadyReads([0, 30, 60, 90]),
          readings,
          workerCount: 2,
        }),
      ),
    ).toBeUndefined();
    expect(
      spanS(
        measureLatest({
          reads: steadyReads([0, 30, 60, 90, 120]),
          readings: [...readings, ...steadyReadings([120])],
          workerCount: 2,
        }),
      ),
    ).toEqual([60, 120]);
  });

  it("measures only a span starting at or after startsFromMs", () => {
    expect(measureSteady([0, 30, 60, 90], 30_001)).toBeUndefined();
    expect(spanS(measureSteady([0, 30, 60, 90], 30_000))).toEqual([30, 90]);
  });

  it("needs a reading at both ends' times, starting at an earlier read when a later one has none", () => {
    const reads = steadyReads([0, 30, 90]);

    expect(
      spanS(
        measureLatest({
          reads,
          readings: steadyReadings([0, 90]),
          workerCount: 2,
        }),
      ),
    ).toEqual([0, 90]);
    expect(
      measureLatest({
        reads,
        readings: steadyReadings([0, 30]),
        workerCount: 2,
      }),
    ).toBeUndefined();
  });

  it("doesn't measure across a dataflow's counter falling, as after a restart", () => {
    expect(
      measureLatest({
        reads: [read(0, cpu("1", [10, 10])), read(60, cpu("1", [2, 1]))],
        readings: [reading(0, [10, 10]), reading(60, [12, 11])],
        workerCount: 2,
      }),
    ).toBeUndefined();
  });

  it("doesn't measure across a worker's counter falling, as when readings start over from the running dataflows' lifetimes", () => {
    expect(
      measureLatest({
        reads: [read(0, cpu("1", [10, 10])), read(60, cpu("1", [40, 25]))],
        readings: [reading(0, [100, 100]), reading(60, [40, 25])],
        workerCount: 2,
      }),
    ).toBeUndefined();
  });

  it("counts a dataflow that finished mid-span toward its row and its worker, with the time its readings carried", () => {
    // An ad-hoc query's dataflow 9 spent 15 s on worker 0 after the start
    // read and finished before the end read. The readings in between carried
    // its time in worker 0's total and in the queries row.
    const measurement = measureLatest({
      reads: [
        {
          ...read(
            0,
            [...cpu("1", [10, 10]), ...cpu("9", [2, 0])],
            [...u1, adhocExport("9")],
          ),
        },
        {
          ...read(60, cpu("1", [40, 25])),
          rowTotals: [
            ["u1", [40 * SECOND_NS, 25 * SECOND_NS]],
            [ADHOC_ROW_ID, [17 * SECOND_NS, 0]],
          ],
          grownDataflows: [
            ["u1", ["1"]],
            [ADHOC_ROW_ID, ["9"]],
          ],
        },
      ],
      readings: [reading(0, [12, 10]), reading(60, [57, 25])],
      workerCount: 2,
    });

    expect(measurement?.workers).toEqual([
      { workerId: 0, busyFraction: 0.75 },
      { workerId: 1, busyFraction: 0.25 },
    ]);
    expect(inSeconds(measurement?.workerTotalsNs)).toEqual([45, 15]);
    expect(
      measurement?.rows.map(({ objectId, dataflowCount, perWorkerNs }) => [
        objectId,
        dataflowCount,
        inSeconds(perWorkerNs),
      ]),
    ).toEqual([
      ["u1", 1, [30, 15]],
      [ADHOC_ROW_ID, 1, [15, 0]],
    ]);
  });

  it("counts the dataflows that grew in any read after the start", () => {
    const adhocAt = (atS: number, dataflowIds: string[], totalS: number) => ({
      ...read(atS, cpu("1", steadyS(atS))),
      rowTotals: [
        ...read(atS, cpu("1", steadyS(atS))).rowTotals,
        [ADHOC_ROW_ID, [totalS * SECOND_NS, 0]] as [string, number[]],
      ],
      grownDataflows: [
        ["u1", ["1"]],
        [ADHOC_ROW_ID, dataflowIds],
      ] as Array<[string, string[]]>,
    });

    const measurement = measureLatest({
      reads: [
        adhocAt(0, ["8"], 1),
        adhocAt(30, ["9"], 3),
        adhocAt(60, ["10"], 6),
      ],
      readings: steadyReadings([0, 30, 60]),
      workerCount: 2,
    });

    expect(
      measurement?.rows.find(({ objectId }) => objectId === ADHOC_ROW_ID),
    ).toMatchObject({ dataflowCount: 2, totalNs: 5 * SECOND_NS });
  });
});

describe("objectSpread", () => {
  const spreadOf = (perWorkerS: number[]) =>
    objectSpread(
      objectCpuRows({
        latest: cpu("1", perWorkerS),
        exports: [{ dataflowId: "1", objectId: "u1" }],
        workerCount: perWorkerS.length,
      }).rows[0],
    );

  it("calls an object under SKEW_RATIO times the other workers' average even", () => {
    expect(spreadOf([39, 20, 20, 20])).toEqual({ kind: "even" });
  });

  it("says an object leans on its hottest worker from SKEW_RATIO times the other workers' average", () => {
    expect(spreadOf([20 * SKEW_RATIO, 20, 20, 20])).toEqual({
      kind: "leans",
      workerId: 0,
      ratio: SKEW_RATIO,
    });
    expect(spreadOf([10, 10, 70, 10])).toEqual({
      kind: "leans",
      workerId: 2,
      ratio: 7,
    });
  });

  it("puts an object all on one worker when the other workers spent nothing on it", () => {
    expect(spreadOf([0, 5, 0, 0])).toEqual({
      kind: "allOn",
      workerId: 1,
      ratio: Infinity,
    });
  });

  it("puts an object all on one worker from ONE_WORKER_RATIO times the other workers' average", () => {
    expect(spreadOf([ONE_WORKER_RATIO, 1, 1, 1])).toEqual({
      kind: "allOn",
      workerId: 0,
      ratio: ONE_WORKER_RATIO,
    });
    expect(spreadOf([ONE_WORKER_RATIO - 1, 1, 1, 1]).kind).toBe("leans");
  });
});

describe("samplesInMeasurement", () => {
  const sample = (atS: number): ProcessMetricsSample => ({
    replicaId: "u2",
    processId: "0",
    occurredAt: new Date(atS * 1000),
    cpuNanoCores: 1e9,
    memoryBytes: null,
    diskBytes: null,
    heapBytes: null,
    heapLimit: null,
  });

  it("keeps samples from a minute before the measurement to its end, and none after", () => {
    const samples = [9, 10, 60, 110, 130, 131, 170].map(sample);

    expect(
      samplesInMeasurement(samples, { startMs: 70_000, endMs: 130_000 }).map(
        ({ occurredAt }) => occurredAt.getTime() / 1000,
      ),
    ).toEqual([10, 60, 110, 130]);
  });
});

describe("verdictObjectId", () => {
  // Worker 2 is the busiest. u20 has the most of its time, u30 the most of
  // worker 0's, and u10 the most time overall.
  const { rows, workerTotalsNs } = objectCpuRows({
    latest: [
      ...cpu("1", [20, 20, 20, 20]),
      ...cpu("2", [2, 2, 30, 2]),
      ...cpu("3", [25, 0, 0, 0]),
    ],
    exports: [
      { dataflowId: "1", objectId: "u10" },
      { dataflowId: "2", objectId: "u20" },
      { dataflowId: "3", objectId: "u30" },
    ],
    workerCount: 4,
  });
  const measurement: Measurement = {
    startMs: 0,
    endMs: 60_000,
    workers: [],
    rows,
    workerTotalsNs,
  };

  it("names the object with the most of the busiest worker's time when the work leans on it", () => {
    expect(
      [
        busy([0.14, 0.12, 0.97, 0.13]),
        busy([0.1, 0.1, 0.5, 0.1]),
        busy([0.02, 0.02, 0.08, 0.02]),
      ].map((workers) => {
        const verdict = workerVerdict(workers);
        return [verdict.kind, verdictObjectId(verdict, measurement)];
      }),
    ).toEqual([
      ["saturated", "u20"],
      ["uneven", "u20"],
      ["light", "u20"],
    ]);
    expect(
      verdictObjectId(workerVerdict(busy([0.55, 0.55, 1, 0.55])), measurement),
    ).toBe("u20");
  });

  it("names the object with the most time overall when the work doesn't lean on one worker", () => {
    expect(
      [
        workerVerdict(busy([0.9, 0.88, 0.98, 0.92])),
        workerVerdict(busy([0.17, 0.16, 0.21, 0.18])),
        cpuVerdict(busy([0.55, 0.54, 0.58, 0.53]), 0.89, 1),
      ].map((verdict) => [verdict.kind, verdictObjectId(verdict, measurement)]),
    ).toEqual([
      ["allBusy", "u10"],
      ["balanced", "u10"],
      ["highCpu", "u10"],
    ]);
  });

  it("names no object for an idle replica", () => {
    const verdict = workerVerdict(busy([0, 0, 0.02, 0]));

    expect(verdict.kind).toBe("idle");
    expect(verdictObjectId(verdict, measurement)).toBeUndefined();
  });
});

describe("markedWorkers", () => {
  it("calls the busiest worker overloaded only when the verdict does", () => {
    expect(
      [
        busy([0.97, 0.14, 0.12, 0.13]),
        busy([1, 0.55, 0.55, 0.55]),
        busy([0.5, 0.1, 0.1, 0.1]),
        busy([0.21, 0.17, 0.16, 0.18]),
      ].map((workers) => markedWorkers(workerVerdict(workers))),
    ).toEqual([
      { workerIds: [0], label: "Overloaded worker" },
      { workerIds: [0], label: "Worker at its core's limit" },
      { workerIds: [0], label: "Busiest worker" },
      undefined,
    ]);
  });

  it("calls out every worker at its limit, not only the busiest", () => {
    expect(
      [
        busy([1, 1, 0.3, 0.3]),
        busy([1, 1, 1, 0.3]),
        busy([1, 1, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2]),
      ].map((workers) => markedWorkers(workerVerdict(workers))),
    ).toEqual([
      { workerIds: [0, 1], label: "Workers at their core's limit" },
      { workerIds: [0, 1, 2], label: "Workers at their core's limit" },
      { workerIds: [0, 1], label: "Overloaded workers" },
    ]);
  });
});

describe("processOfWorker", () => {
  it("numbers workers process by process", () => {
    expect([0, 61, 62, 123].map((w) => processOfWorker(w, 62))).toEqual([
      0, 0, 1, 1,
    ]);
  });
});

describe("replicaCpuPoints", () => {
  const sample = (
    processId: string,
    atS: number,
    cores: number | null,
  ): ProcessMetricsSample => ({
    replicaId: "u1",
    processId,
    occurredAt: new Date(atS * 1000),
    cpuNanoCores: cores === null ? null : cores * 1e9,
    memoryBytes: null,
    diskBytes: null,
    heapBytes: null,
    heapLimit: null,
  });

  it("averages one scrape's processes as a percent of a process's allocation", () => {
    expect(
      replicaCpuPoints(
        [
          sample("0", 60, 3),
          sample("1", 61, 1),
          sample("0", 120, 2),
          sample("1", 120, null),
        ],
        {
          processes: 2,
          workers: 4,
          cpuNanoCores: 4e9,
          memoryBytes: 0,
          diskBytes: 0,
        },
      ),
    ).toEqual([
      { atMs: 60_000, percent: 50 },
      { atMs: 120_000, percent: 50 },
    ]);
  });
});

describe("workerLabel", () => {
  const replica = (processes: number): WorkerReplica => ({
    id: "u1",
    name: "r1",
    size: "large",
    color: "#7c5cff",
    allocation: {
      processes,
      workers: 31,
      cpuNanoCores: 31e9,
      memoryBytes: 0,
      diskBytes: 0,
    },
  });

  it("names a worker's process only when the replica runs more than one", () => {
    expect(workerLabel(67, replica(4))).toEqual("w67 (process 2)");
    expect(workerLabel(7, replica(1))).toEqual("w7");
  });
});

describe("runsStorageDataflow", () => {
  const object = (
    objectType: string,
    sourceType: string | null = null,
  ): DatabaseObject => ({
    id: "u1",
    name: "o",
    objectType,
    schemaId: "u2",
    schemaName: "public",
    databaseId: "u3",
    databaseName: "materialize",
    sourceType,
    isWebhookTable: null,
    clusterId: "u4",
    clusterName: "c",
  });

  it("counts sources and sinks but not webhook sources or compute objects", () => {
    expect(runsStorageDataflow(object("source", "kafka"))).toBe(true);
    expect(runsStorageDataflow(object("sink"))).toBe(true);
    expect(runsStorageDataflow(object("source", "webhook"))).toBe(false);
    expect(runsStorageDataflow(object("index"))).toBe(false);
    expect(runsStorageDataflow(object("materialized-view"))).toBe(false);
  });
});
