// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { greatest, max, mean } from "d3";

import {
  ProcessMetricsSample,
  SizeAllocation,
} from "~/api/materialize/cluster/replicaProcessMetrics";

export type ProcessMetric = "cpu" | "memory";

export type ProcessReading = Record<ProcessMetric, number | null>;

export interface ProcessBucket {
  bucketStart: number;
  bucketEnd: number;
  /** Each reporting process's peak reading in the bucket, by process id. */
  readings: Map<string, ProcessReading>;
}

const percentOf = (value: number | null, total: number | null) =>
  value === null || !total ? null : (value / total) * 100;

const greatestOf = (a: number | null | undefined, b: number | null) =>
  a === null || a === undefined ? b : b === null ? a : Math.max(a, b);

export interface ProcessSampleReading extends ProcessReading {
  processId: string;
  atMs: number;
}

/**
 * Each sample from `startMs` on, in percent of one process's allocation: CPU
 * of its CPU allocation, memory of its heap limit, or of its memory allocation
 * where the replica never reports one.
 */
export function readProcessSamples({
  samples,
  allocation,
  startMs,
}: {
  samples: ProcessMetricsSample[];
  allocation: SizeAllocation;
  startMs: number;
}): ProcessSampleReading[] {
  const reportsHeapLimit = samples.some(({ heapLimit }) => heapLimit !== null);
  return samples.flatMap((sample) => {
    const atMs = sample.occurredAt.getTime();
    if (atMs < startMs) return [];
    return [
      {
        processId: sample.processId,
        atMs,
        cpu: percentOf(sample.cpuNanoCores, allocation.cpuNanoCores),
        // NOTE: a failed usage fetch leaves heap and its limit null while the
        // memory reading survives, near the whole allocation since it counts
        // page cache. Falling back to it there would show a false near-OOM
        // process.
        memory: reportsHeapLimit
          ? percentOf(sample.heapBytes, sample.heapLimit)
          : percentOf(sample.memoryBytes, allocation.memoryBytes),
      },
    ];
  });
}

/** The processes with a sample, in numeric order. */
export const processIdsOf = (readings: ProcessSampleReading[]) =>
  [...new Set(readings.map(({ processId }) => processId))].sort(
    (a, b) => Number(a) - Number(b),
  );

/** Each bucket's peak reading per process, in time order. */
export function bucketProcessReadings(
  readings: ProcessSampleReading[],
  bucketSizeMs: number,
): ProcessBucket[] {
  const byBucket = new Map<number, Map<string, ProcessReading>>();
  for (const { processId, atMs, cpu, memory } of readings) {
    const bucketStart = Math.floor(atMs / bucketSizeMs) * bucketSizeMs;
    const peaks = byBucket.get(bucketStart) ?? new Map();
    const previous = peaks.get(processId);
    peaks.set(processId, {
      cpu: greatestOf(previous?.cpu, cpu),
      memory: greatestOf(previous?.memory, memory),
    });
    byBucket.set(bucketStart, peaks);
  }
  return [...byBucket]
    .sort(([a], [b]) => a - b)
    .map(([bucketStart, peaks]) => ({
      bucketStart,
      bucketEnd: bucketStart + bucketSizeMs,
      readings: peaks,
    }));
}

export interface ProcessStats {
  /** Null when the process never reported this metric in the window. */
  average: number | null;
  peak: number | null;
}

export interface ProcessSummary {
  processId: string;
  cpu: ProcessStats;
  memory: ProcessStats;
}

/**
 * Each process's mean and peak over its samples. Bucket peaks would inflate a
 * bursty process's mean more the wider the window's buckets.
 */
export function summarizeProcesses(
  processIds: string[],
  readings: ProcessSampleReading[],
): ProcessSummary[] {
  const stats = (processId: string, metric: ProcessMetric): ProcessStats => {
    const values = readings.flatMap((reading) =>
      reading.processId !== processId || reading[metric] === null
        ? []
        : [reading[metric]],
    );
    return { average: mean(values) ?? null, peak: max(values) ?? null };
  };
  return processIds.map((processId) => ({
    processId,
    cpu: stats(processId, "cpu"),
    memory: stats(processId, "memory"),
  }));
}

export interface BusiestProcess {
  processId: string;
  average: number;
  /** The mean of the other processes' averages. */
  othersAverage: number;
  otherCount: number;
}

/**
 * The process with the highest average for `metric`, against the others.
 * Undefined unless at least two processes reported it.
 */
export function busiestProcess(
  summaries: ProcessSummary[],
  metric: ProcessMetric,
): BusiestProcess | undefined {
  const reported = summaries.flatMap(({ processId, [metric]: stats }) =>
    stats.average === null ? [] : [{ processId, average: stats.average }],
  );
  const busiest = greatest(reported, ({ average }) => average);
  if (!busiest || reported.length < 2) return undefined;
  const others = reported.filter((process) => process !== busiest);
  return {
    processId: busiest.processId,
    average: busiest.average,
    othersAverage: mean(others, ({ average }) => average) ?? 0,
    otherCount: others.length,
  };
}
