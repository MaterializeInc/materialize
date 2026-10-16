// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { greatest, group, max, mean } from "d3";

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

/** Whether memory reads against the heap limit, not the memory allocation. */
export const reportsHeapLimit = (samples: ProcessMetricsSample[]) =>
  samples.some(({ heapLimit }) => heapLimit !== null);

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
  const hasHeapLimit = reportsHeapLimit(samples);
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
        memory: hasHeapLimit
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

/**
 * Each bucket's peak reading per process from `startMs` on, in time order.
 * Buckets align to the epoch, so the first can begin before `startMs`; it
 * begins at `startMs` instead, which keeps it inside the window's time axis.
 */
export function bucketProcessReadings(
  readings: ProcessSampleReading[],
  bucketSizeMs: number,
  startMs = Number.NEGATIVE_INFINITY,
): ProcessBucket[] {
  const byBucket = new Map<number, Map<string, ProcessReading>>();
  for (const { processId, atMs, cpu, memory } of readings) {
    if (atMs < startMs) continue;
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
      bucketStart: Math.max(bucketStart, startMs),
      bucketEnd: bucketStart + bucketSizeMs,
      readings: peaks,
    }));
}

/** Which reading the drawer compares processes by. */
export type ProcessStatistic = "current" | "peak" | "average";

export interface ProcessStats {
  /**
   * The reading in the newest round of samples. Null when the process has
   * none there, as when it's down.
   */
  current: number | null;
  peak: number | null;
  average: number | null;
}

export interface ProcessSummary {
  processId: string;
  cpu: ProcessStats;
  memory: ProcessStats;
}

/**
 * Each process's latest reading, peak and mean over its samples. Bucket peaks
 * would inflate a bursty process's mean more the wider the window's buckets.
 */
export function summarizeProcesses(
  processIds: string[],
  readings: ProcessSampleReading[],
): ProcessSummary[] {
  // The controller stamps every process's sample in a round with one time,
  // and writes nulls for a process it couldn't read (`record_replica_metrics`
  // in mz-controller), so a down process's last reading is older than this.
  const latestRoundMs = max(readings, ({ atMs }) => atMs);
  const readingsByProcess = group(readings, ({ processId }) => processId);
  const stats = (
    own: ProcessSampleReading[],
    metric: ProcessMetric,
  ): ProcessStats => {
    const reported = own.flatMap((reading) => {
      const value = reading[metric];
      return value === null ? [] : [{ atMs: reading.atMs, value }];
    });
    return {
      current:
        reported.find(({ atMs }) => atMs === latestRoundMs)?.value ?? null,
      peak: max(reported, ({ value }) => value) ?? null,
      average: mean(reported, ({ value }) => value) ?? null,
    };
  };
  return processIds.map((processId) => {
    const own = readingsByProcess.get(processId) ?? [];
    return { processId, cpu: stats(own, "cpu"), memory: stats(own, "memory") };
  });
}

export interface BusiestProcess {
  processId: string;
  value: number;
  /** The mean of the other processes' values. */
  othersMean: number;
  otherCount: number;
}

/** A busiest process or worker within this factor of the others' mean reads as even. */
export const EVEN_RATIO = 1.25;
// Near idle, a gap too small to matter is still a large ratio (0.3% against
// 0.1% is 3×), so a lead also has to clear this many percentage points.
const MIN_LEAD_POINTS = 5;

/** Names the processes a busiest one is compared with. */
export const otherProcessesLabel = (count: number) =>
  count === 1 ? "the other process" : `the other ${count} processes`;

/** Whether the busiest process leads the others by enough to name it. */
export const standsOut = (busiest: BusiestProcess | undefined) =>
  busiest !== undefined &&
  busiest.value - busiest.othersMean >= MIN_LEAD_POINTS &&
  busiest.value >= busiest.othersMean * EVEN_RATIO;

/**
 * The process with the highest `statistic` for `metric`, against the others.
 * Undefined unless at least two processes reported it.
 */
export function busiestProcess(
  summaries: ProcessSummary[],
  metric: ProcessMetric,
  statistic: ProcessStatistic,
): BusiestProcess | undefined {
  const reported = summaries.flatMap(({ processId, [metric]: stats }) => {
    const value = stats[statistic];
    return value === null ? [] : [{ processId, value }];
  });
  const busiest = greatest(reported, ({ value }) => value);
  if (!busiest || reported.length < 2) return undefined;
  const others = reported.filter((process) => process !== busiest);
  return {
    processId: busiest.processId,
    value: busiest.value,
    othersMean: mean(others, ({ value }) => value) ?? 0,
    otherCount: others.length,
  };
}
