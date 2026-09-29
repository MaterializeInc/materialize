// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { DataPoint } from "~/components/FreshnessGraph/types";
import { assignLineColors } from "~/components/ThresholdLineGraph/thresholdLineGraphHelpers";
import { ThresholdLineSeries } from "~/components/ThresholdLineGraph/types";
import { FreshnessObject } from "~/platform/clusters/queries";

/** Which statistic the threshold judges an object by. */
export type Predicate = "current" | "peak" | "p90";

export const PREDICATE_LABELS: Record<Predicate, string> = {
  current: "right now",
  peak: "at any moment",
  p90: ">10% of readings (p90)",
};

export interface FreshnessRow {
  key: string;
  objectName: string;
  /** Qualified prefix, shown under the name. */
  namespace: string;
  objectType: string;
  hydratedReplicas: number;
  totalReplicas: number;
  current: number | null;
  peak: number | null;
  p90: number | null;
  /** The statistic the active predicate judges this row by. */
  breachValue: number | null;
  breaching: boolean;
  /** The color its line is drawn in, or undefined when it is not drawn. */
  color: string | undefined;
}

export interface ObjectStats {
  current: number | null;
  peak: number | null;
  p90: number | null;
}

/**
 * The three statistics a predicate can judge, from one pass over the window.
 *
 * `p90` is nearest-rank: sort the readings and take the value 90% of the way
 * up. The query hands us 60 readings whatever range is selected, and
 * ceil(0.90 * 60) lands on the 54th, so p90 is the 7th largest: distinct from
 * the peak, and it discards a short spike the way the peak cannot.
 *
 * NOTE: p99 would not survive that. ceil(0.99 * 60) is the 60th, so over these
 * readings p99 *is* the peak and the two predicates would select identically.
 * Each reading is also already a maximum over its bin, so these are
 * percentiles of maxima rather than of the underlying lag.
 */
export function computeStats(
  yAccessor: (d: DataPoint) => number | null,
  data: DataPoint[],
): ObjectStats {
  const values: number[] = [];
  for (const d of data) {
    const v = yAccessor(d);
    if (v !== null) values.push(v);
  }
  if (values.length === 0) {
    return { current: null, peak: null, p90: null };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const rank = Math.max(0, Math.ceil(0.9 * sorted.length) - 1);

  return {
    current: yAccessor(data[data.length - 1]),
    peak: sorted[sorted.length - 1],
    p90: sorted[rank],
  };
}

const statFor = (stats: ObjectStats, predicate: Predicate) =>
  predicate === "current"
    ? stats.current
    : predicate === "peak"
      ? stats.peak
      : stats.p90;

/**
 * Attaches the statistic the active predicate judges to each line.
 *
 * The graph asks its caller for this rather than deriving it, because which
 * statistic deserves judging is policy. Changing the predicate re-ranks the
 * lines and so re-colors them, which is why the colors move when the menu does
 * but hold still while the threshold is dragged.
 */
export function judgeLines(
  lines: {
    key: string;
    label?: string;
    yAccessor: (d: DataPoint) => number | null;
  }[],
  data: DataPoint[],
  predicate: Predicate,
  statsByKey: Map<string, ObjectStats>,
): ThresholdLineSeries<DataPoint>[] {
  return lines.map((line) => {
    const stats =
      statsByKey.get(line.key) ?? computeStats(line.yAccessor, data);
    return {
      key: line.key,
      label: line.label,
      yAccessor: line.yAccessor,
      breachValue: statFor(stats, predicate),
    };
  });
}

export function buildStats(
  lines: { key: string; yAccessor: (d: DataPoint) => number | null }[],
  data: DataPoint[],
): Map<string, ObjectStats> {
  return new Map(
    lines.map((line) => [line.key, computeStats(line.yAccessor, data)]),
  );
}

/**
 * Table rows for the objects behind the graph, worst first.
 *
 * `selectedKeys` is a union with the breaching set, never a replacement: a row
 * picked by hand adds a line to the graph without removing the ones the
 * threshold chose. Replacement is what would make the threshold and the
 * selection fight over the same channel.
 */
export function buildFreshnessRows(
  judged: ThresholdLineSeries<DataPoint>[],
  statsByKey: Map<string, ObjectStats>,
  objectsById: Map<string, FreshnessObject>,
  threshold: number,
  selectedKeys: ReadonlySet<string>,
): FreshnessRow[] {
  const colors = assignLineColors(judged);

  return judged
    .map((line) => {
      const stats = statsByKey.get(line.key) ?? {
        current: null,
        peak: null,
        p90: null,
      };
      const object = objectsById.get(line.key);
      const breaching =
        line.breachValue !== null && line.breachValue > threshold;
      const drawn = breaching || selectedKeys.has(line.key);

      return {
        key: line.key,
        objectName: object?.objectName ?? line.label ?? line.key,
        namespace: [object?.databaseName, object?.schemaName]
          .filter(Boolean)
          .join("."),
        objectType: object?.objectType ?? "",
        hydratedReplicas: object?.hydratedReplicas ?? 0,
        totalReplicas: object?.totalReplicas ?? 0,
        current: stats.current,
        peak: stats.peak,
        p90: stats.p90,
        breachValue: line.breachValue,
        breaching,
        color: drawn ? colors.get(line.key) : undefined,
      };
    })
    .sort(
      (a, b) =>
        (b.breachValue ?? -1) - (a.breachValue ?? -1) ||
        a.objectName.localeCompare(b.objectName),
    );
}

export type SortKey = "objectName" | "objectType" | "current" | "peak" | "p90";

/** Sorts rows, keeping nulls last whichever direction is asked for. */
export function sortRows(
  rows: FreshnessRow[],
  key: SortKey,
  direction: 1 | -1,
): FreshnessRow[] {
  return [...rows].sort((a, b) => {
    const x = a[key];
    const y = b[key];
    if (x === null && y === null) return 0;
    if (x === null) return 1;
    if (y === null) return -1;
    if (typeof x === "string" && typeof y === "string") {
      return x.localeCompare(y) * direction;
    }
    return ((x as number) - (y as number)) * direction;
  });
}
