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
  summarizeProcesses,
} from "./processReadingsModel";

const MINUTE = 60_000;
const ALLOCATION = {
  processes: 3,
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

  it("averages and peaks each process over the samples it reported", () => {
    expect(summaries.map(({ processId, cpu }) => [processId, cpu])).toEqual([
      ["0", { average: 20, peak: 30 }],
      ["1", { average: 80, peak: 90 }],
      ["2", { average: 30, peak: 30 }],
    ]);
  });

  it("names the process with the highest average against the others", () => {
    expect(busiestProcess(summaries, "cpu")).toEqual({
      processId: "1",
      average: 80,
      othersAverage: 25,
      otherCount: 2,
    });
  });

  it("names no process when fewer than two reported", () => {
    expect(busiestProcess(summaries, "memory")).toBeUndefined();
  });

  it("names the steady process over a bursty one however wide the buckets", () => {
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

    expect(busiestProcess(daySummaries, "cpu")?.processId).toEqual("1");
    expect(daySummaries[0].cpu).toEqual({ average: 5, peak: 100 });
  });
});
