// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ProcessMetricsSample } from "~/api/materialize/cluster/replicaProcessMetrics";

import {
  bucketProcessReadings,
  busiestProcess,
  processIdsOf,
  readProcessSamples,
  standsOut,
  summarizeProcesses,
} from "./processReadingsModel";

const MINUTE = 60_000;
const ALLOCATION = {
  processes: 3,
  workers: 1,
  cpuNanoCores: 1e9,
  memoryBytes: 1000,
  diskBytes: 0,
};

const sample = (
  processId: string,
  atMinute: number,
  overrides: Partial<ProcessMetricsSample> = {},
): ProcessMetricsSample => ({
  replicaId: "u1",
  processId,
  occurredAt: new Date(atMinute * MINUTE),
  cpuNanoCores: null,
  memoryBytes: null,
  diskBytes: null,
  heapBytes: null,
  heapLimit: null,
  ...overrides,
});

const read = (samples: ProcessMetricsSample[], startMs = 0) =>
  readProcessSamples({ samples, allocation: ALLOCATION, startMs });

describe("readProcessSamples", () => {
  it("reads each sample as a share of one process's allocation, from the window's start", () => {
    const readings = read(
      [
        sample("0", 1, { cpuNanoCores: 2e8 }),
        sample("0", 3, { cpuNanoCores: 6e8 }),
      ],
      2 * MINUTE,
    );

    expect(readings).toEqual([
      { processId: "0", atMs: 3 * MINUTE, cpu: 60, memory: null },
    ]);
  });

  it("reads memory against the heap limit, not a failed fetch's page cache", () => {
    const readings = read([
      sample("0", 0, { heapBytes: 50, heapLimit: 100, memoryBytes: 300 }),
      // Usage fetch failed: heap and its limit null, memory near the whole
      // allocation.
      sample("1", 0, { memoryBytes: 990 }),
    ]);

    expect(readings.map(({ memory }) => memory)).toEqual([50, null]);
  });

  it("falls back to the share of the memory allocation without heap limits", () => {
    expect(read([sample("0", 0, { memoryBytes: 250 })])[0].memory).toEqual(25);
  });

  it("lists processes numerically", () => {
    expect(
      processIdsOf(read([sample("10", 0), sample("2", 0), sample("2", 1)])),
    ).toEqual(["2", "10"]);
  });
});

describe("bucketProcessReadings", () => {
  it("keeps each process's peak in a bucket, buckets in time order", () => {
    const buckets = bucketProcessReadings(
      read([
        sample("0", 3, { cpuNanoCores: 1e8 }),
        sample("0", 0, { cpuNanoCores: 2e8 }),
        sample("0", 1, { cpuNanoCores: 6e8 }),
        sample("1", 1, { cpuNanoCores: 4e8 }),
      ]),
      2 * MINUTE,
    );

    expect(
      buckets.map(({ bucketStart, readings }) => [
        bucketStart / MINUTE,
        readings.get("0")?.cpu,
        readings.get("1")?.cpu,
      ]),
    ).toEqual([
      [0, 60, 40],
      [2, 10, undefined],
    ]);
  });

  it("starts the first bucket at the window's start, with only the readings since", () => {
    const buckets = bucketProcessReadings(
      read([
        sample("0", 1, { cpuNanoCores: 9e8 }),
        sample("0", 3, { cpuNanoCores: 1e8 }),
        sample("0", 5, { cpuNanoCores: 2e8 }),
      ]),
      4 * MINUTE,
      3 * MINUTE,
    );

    expect(
      buckets.map(({ bucketStart, bucketEnd, readings }) => [
        bucketStart / MINUTE,
        bucketEnd / MINUTE,
        readings.get("0")?.cpu,
      ]),
    ).toEqual([
      [3, 4, 10],
      [4, 8, 20],
    ]);
  });
});

describe("summarizeProcesses and busiestProcess", () => {
  const readings = read([
    sample("0", 0, { cpuNanoCores: 3e8 }),
    sample("1", 0, { cpuNanoCores: 9e8 }),
    sample("2", 0, { cpuNanoCores: 3e8 }),
    sample("0", 2, { cpuNanoCores: 1e8 }),
    sample("1", 2, { cpuNanoCores: 7e8 }),
    // Process 2's fetch failed here.
    sample("2", 2),
  ]);
  const summaries = summarizeProcesses(processIdsOf(readings), readings);

  it("reads each process's newest-round, peak and mean readings over the samples it reported", () => {
    expect(summaries.map(({ processId, cpu }) => [processId, cpu])).toEqual([
      ["0", { current: 10, peak: 30, average: 20 }],
      ["1", { current: 70, peak: 90, average: 80 }],
      ["2", { current: null, peak: 30, average: 30 }],
    ]);
  });

  it("names the process with the highest statistic against the others", () => {
    expect(busiestProcess(summaries, "cpu", "average")).toEqual({
      processId: "1",
      value: 80,
      othersMean: 25,
      otherCount: 2,
    });
    expect(busiestProcess(summaries, "cpu", "current")).toEqual({
      processId: "1",
      value: 70,
      othersMean: 10,
      otherCount: 1,
    });
  });

  it("reads no current value for a process that is down in the newest round", () => {
    // Process 1 read 94% until it went down, and its rows since are null.
    const downReadings = read([
      sample("0", 0, { cpuNanoCores: 3e8 }),
      sample("1", 0, { cpuNanoCores: 9.4e8 }),
      sample("0", 1, { cpuNanoCores: 3e8 }),
      sample("1", 1),
    ]);
    const downSummaries = summarizeProcesses(
      processIdsOf(downReadings),
      downReadings,
    );

    expect(downSummaries.map(({ cpu }) => cpu.current)).toEqual([30, null]);
    expect(busiestProcess(downSummaries, "cpu", "current")).toBeUndefined();
    expect(busiestProcess(downSummaries, "cpu", "peak")).toMatchObject({
      processId: "1",
      value: 94,
    });
  });

  it("names no process when fewer than two reported", () => {
    expect(busiestProcess(summaries, "memory", "peak")).toBeUndefined();
  });

  it("names a climbing process by its latest and peak readings, though its mean matches a steady one", () => {
    // Process 0 climbs from 10% to 90%, process 1 holds 50%: equal means.
    const climb = [1e8, 3e8, 5e8, 7e8, 9e8].flatMap((cpuNanoCores, minute) => [
      sample("0", minute, { cpuNanoCores }),
      sample("1", minute, { cpuNanoCores: 5e8 }),
    ]);
    const climbReadings = read(climb);
    const climbSummaries = summarizeProcesses(
      processIdsOf(climbReadings),
      climbReadings,
    );

    expect(busiestProcess(climbSummaries, "cpu", "average")).toMatchObject({
      value: 50,
      othersMean: 50,
    });
    expect(busiestProcess(climbSummaries, "cpu", "current")).toMatchObject({
      processId: "0",
      value: 90,
      othersMean: 50,
    });
    expect(busiestProcess(climbSummaries, "cpu", "peak")).toMatchObject({
      processId: "0",
      value: 90,
      othersMean: 50,
    });
  });

  it("names the steady process over a bursty one on average however wide the buckets", () => {
    // A day of per-minute samples, as the 24h window buckets them: process 0
    // bursts to 100% one minute in twenty and idles, process 1 holds 40%.
    const day = Array.from({ length: 24 * 60 }, (_, minute) => [
      sample("0", minute, { cpuNanoCores: minute % 20 === 0 ? 1e9 : 0 }),
      sample("1", minute, { cpuNanoCores: 4e8 }),
    ]).flat();
    const dayReadings = read(day);
    const daySummaries = summarizeProcesses(
      processIdsOf(dayReadings),
      dayReadings,
    );

    expect(busiestProcess(daySummaries, "cpu", "average")?.processId).toEqual(
      "1",
    );
    expect(daySummaries[0].cpu).toMatchObject({ average: 5, peak: 100 });
  });
});

describe("standsOut", () => {
  const busiest = (value: number, othersMean: number) => ({
    processId: "0",
    value,
    othersMean,
    otherCount: 1,
  });

  it("reads idle processes as even", () => {
    expect(standsOut(busiest(0, 0))).toBe(false);
  });

  it("reads a near-idle lead as even, however large its ratio", () => {
    expect(standsOut(busiest(0.3, 0.1))).toBe(false);
  });

  it("needs a lead to clear both the ratio and the gap", () => {
    expect(standsOut(busiest(10, 0))).toBe(true);
    expect(standsOut(busiest(90, 30))).toBe(true);
    // 10 points ahead, but only 1.2× the others.
    expect(standsOut(busiest(60, 50))).toBe(false);
  });
});
