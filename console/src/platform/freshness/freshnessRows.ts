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

import { HydrationCounts } from "./queries";

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
  /** The object reported an unreadable lag; its numbers are not comparable. */
  notQueryable: boolean;
  /** The color its line is drawn in, or undefined when it is not drawn. */
  color: string | undefined;
}

export interface ObjectStats {
  current: number | null;
  peak: number | null;
  p90: number | null;
  /**
   * The object reported an unreadable lag at some point in the window.
   * Counts as a breach at any threshold.
   */
  notQueryable: boolean;
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
  key: string,
  data: DataPoint[],
  latest: number | null | undefined,
): ObjectStats {
  const values: number[] = [];
  let notQueryable = false;

  for (const d of data) {
    const reading = d.lag[key];
    // No reading for this object in this bin says nothing about it.
    if (reading === undefined) continue;
    // A reading whose lag is NULL is a measurement, and its answer is that the
    // object could not be read. Reading it through the graph's accessor would
    // hand back 0, which scores the worst state as the best one.
    if (!reading.queryable) {
      notQueryable = true;
      continue;
    }
    values.push(reading.totalMs);
  }

  if (values.length === 0) {
    return { current: latest ?? null, peak: null, p90: null, notQueryable };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const rank = Math.max(0, Math.ceil(0.9 * sorted.length) - 1);

  return {
    // The newest reading, not the newest bin. A bin reports the worst reading
    // in its span, so at a 24 hour range the last one answers "the worst of
    // the last 24 minutes" when the question asked was "right now".
    current: latest ?? null,
    peak: sorted[sorted.length - 1],
    p90: sorted[rank],
    notQueryable,
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
      statsByKey.get(line.key) ?? computeStats(line.key, data, undefined);
    return {
      key: line.key,
      label: line.label,
      yAccessor: line.yAccessor,
      // `Infinity` exceeds every threshold and sorts ahead of every measured
      // lag, which is what an unreadable object deserves. It never reaches the
      // screen: `FreshnessRow.notQueryable` is what the table renders from.
      breachValue: stats.notQueryable ? Infinity : statFor(stats, predicate),
    };
  });
}

export function buildStats(
  lines: { key: string }[],
  data: DataPoint[],
  latestByObjectId: Map<string, number | null>,
): Map<string, ObjectStats> {
  return new Map(
    lines.map((line) => [
      line.key,
      computeStats(line.key, data, latestByObjectId.get(line.key)),
    ]),
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
export function buildFreshnessRows({
  judged,
  statsByKey,
  objectsById,
  hydrationByObjectId,
  threshold,
  selectedKeys,
}: {
  judged: ThresholdLineSeries<DataPoint>[];
  statsByKey: Map<string, ObjectStats>;
  objectsById: Map<string, FreshnessObject>;
  hydrationByObjectId: Map<string, HydrationCounts>;
  threshold: number;
  selectedKeys: ReadonlySet<string>;
}): FreshnessRow[] {
  const colors = assignLineColors(judged);

  return judged
    .map((line) => {
      const stats = statsByKey.get(line.key) ?? {
        current: null,
        peak: null,
        p90: null,
        notQueryable: false,
      };
      const object = objectsById.get(line.key);
      const hydration = hydrationByObjectId.get(line.key);
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
        hydratedReplicas: hydration?.hydratedReplicas ?? 0,
        totalReplicas: hydration?.totalReplicas ?? 0,
        current: stats.current,
        peak: stats.peak,
        p90: stats.p90,
        breachValue: line.breachValue,
        breaching,
        notQueryable: stats.notQueryable,
        color: drawn ? colors.get(line.key) : undefined,
      };
    })
    .sort(
      (a, b) =>
        (b.breachValue ?? -1) - (a.breachValue ?? -1) ||
        a.objectName.localeCompare(b.objectName),
    );
}
