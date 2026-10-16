// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  ADHOC_ROW_ID,
  cpuVerdict,
  DataflowWorkerCpu,
  dominantObject,
  hasCounterReset,
  hasDataflowCounterReset,
  lifetimeWorkerSkew,
  objectCpuRows,
  processOfWorker,
  replicaCpuPoints,
  topObjectOnWorker,
  verdictWindowStart,
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
  it("flags a reading where any worker's counter fell, as when a dataflow is dropped", () => {
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

  it("starts a new line after a reset", () => {
    expect(
      workerBusySeries(
        [
          reading(0, [0]),
          reading(5, [4]),
          { ...reading(10, [1]), afterReset: true },
          reading(15, [5]),
        ],
        0,
      ).map(({ atMs, afterGap }) => [atMs, Boolean(afterGap)]),
    ).toEqual([
      [5000, false],
      [15000, true],
    ]);
  });
});

describe("verdictWindowStart", () => {
  const readings = [0, 60, 120, 180, 240, 300, 360].map((atS) =>
    reading(atS, [atS]),
  );

  it("reaches back as far as the window", () => {
    expect(verdictWindowStart(readings, 200_000)?.atMs).toEqual(180_000);
  });

  it("doesn't reach back past a reset, since counters before it don't compare", () => {
    const withReset = readings.map((r) =>
      r.atMs === 300_000 ? { ...r, afterReset: true } : r,
    );
    expect(verdictWindowStart(withReset, 200_000)?.atMs).toEqual(300_000);
  });

  it("spans a pause, since counters keep counting through it", () => {
    const withGap = readings.map((r) =>
      r.atMs === 300_000 ? { ...r, afterGap: true } : r,
    );
    expect(verdictWindowStart(withGap, 200_000)?.atMs).toEqual(180_000);
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
      cpuVerdict(busy([0.5, 0.5, 0.5, 0.5]), 0.8, workersPerCore(4, 16e9)),
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
  const cpu = (dataflowId: string, perWorkerS: number[]): DataflowWorkerCpu[] =>
    perWorkerS.map((s, workerId) => ({
      dataflowId,
      workerId,
      elapsedNs: s * SECOND_NS,
    }));

  it("measures each object's hottest worker against an even share", () => {
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
        .map(({ objectId, hottestWorkerId, ratio, extraNs }) => [
          objectId,
          hottestWorkerId,
          ratio,
          extraNs / SECOND_NS,
        ]),
    ).toEqual([
      ["u10", 0, 2.8, 45],
      ["u20", 0, 1, 0],
    ]);
  });

  it("measures only the time since the baseline read", () => {
    const { rows } = objectCpuRows({
      baseline: cpu("1", [1000, 1000]),
      latest: cpu("1", [1030, 1010]),
      exports,
      workerCount: 2,
    });

    expect(rows[0].perWorkerNs.map((ns) => ns / SECOND_NS)).toEqual([30, 10]);
  });

  it("pools temporary dataflows for ad-hoc queries into one row", () => {
    const { rows } = objectCpuRows({
      latest: [...cpu("7", [3, 1]), ...cpu("8", [1, 1])],
      exports: [
        { dataflowId: "7", objectId: "t41" },
        { dataflowId: "8", objectId: "t42" },
      ],
      workerCount: 2,
    });

    expect(
      rows.map(({ objectId, dataflowCount, perWorkerNs }) => [
        objectId,
        dataflowCount,
        perWorkerNs.map((ns) => ns / SECOND_NS),
      ]),
    ).toEqual([[ADHOC_ROW_ID, 2, [4, 2]]]);
  });

  it("names an object that takes most of the replica's dataflow time", () => {
    const heavy = objectCpuRows({
      latest: [
        ...cpu("1", [60, 60]),
        ...cpu("2", [20, 20]),
        ...cpu("99", [20, 20]),
      ],
      exports,
      workerCount: 2,
    });
    const spread = objectCpuRows({
      latest: [
        ...cpu("1", [40, 40]),
        ...cpu("2", [35, 35]),
        ...cpu("99", [25, 25]),
      ],
      exports,
      workerCount: 2,
    });

    expect(dominantObject(heavy.rows, heavy.workerTotalsNs)).toEqual({
      objectId: "u10",
      share: 0.6,
    });
    expect(dominantObject(spread.rows, spread.workerTotalsNs)).toBeUndefined();
  });

  it("counts system and transient dataflows in each worker's total without a row", () => {
    const { rows, workerTotalsNs } = objectCpuRows({
      latest: [...cpu("1", [6, 2]), ...cpu("99", [4, 0])],
      exports,
      workerCount: 2,
    });

    expect(rows.map((row) => row.objectId)).toEqual(["u10"]);
    expect(workerTotalsNs.map((ns) => ns / SECOND_NS)).toEqual([10, 2]);
    expect(topObjectOnWorker(rows, workerTotalsNs, 0)).toEqual({
      objectId: "u10",
      share: 0.6,
    });
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
    atS: number,
    cores: number | null,
  ): Parameters<typeof replicaCpuPoints>[0][number] => ({
    occurredAt: new Date(atS * 1000),
    cpuNanoCores: cores === null ? null : cores * 1e9,
  });

  it("averages one scrape's processes as a percent of a process's allocation", () => {
    expect(
      replicaCpuPoints(
        [sample(60, 3), sample(61, 1), sample(120, 2), sample(120, null)],
        4e9,
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
    processes,
    workersPerProcess: 31,
    cpuNanoCoresPerProcess: 31e9,
  });

  it("names a worker's process only when the replica runs more than one", () => {
    expect(workerLabel(67, replica(4))).toEqual("w67 (process 2)");
    expect(workerLabel(7, replica(1))).toEqual("w7");
  });
});
