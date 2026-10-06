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
 *
 * A reading that could not be taken counts as `UNREADABLE`, which is
 * `Infinity`, so it ranks above every measured lag and lands in the
 * denominator. It therefore counts once, the same as one reading over the
 * threshold: six unreadable readings out of sixty no more breach p90 than six
 * slow ones do. A flag that overrode the percentile instead would let a single
 * unreadable reading outrank fifty-nine healthy ones.
 */
export function computeStats(
  key: string,
  data: DataPoint[],
  latest: number | null | undefined,
): ObjectStats {
  const values: number[] = [];

  for (const d of data) {
    const reading = d.lag[key];
    // No reading at all says nothing about the object, so it is not a reading.
    if (reading === undefined) continue;
    values.push(reading.queryable ? reading.totalMs : UNREADABLE);
  }

  if (values.length === 0) {
    return { current: currentFrom(latest), peak: null, p90: null };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const rank = Math.max(0, Math.ceil(0.9 * sorted.length) - 1);

  return {
    // The newest reading, not the newest bin. A bin reports the worst reading
    // in its span, so at a 24 hour range the last one answers "the worst of
    // the last 24 minutes" when the question asked was "right now".
    current: currentFrom(latest),
    peak: sorted[sorted.length - 1],
    p90: sorted[rank],
  };
}

/**
 * A reading whose lag came back NULL: the object could not be read.
 *
 * `Infinity` rather than a flag, so it sorts, ranks and compares against a
 * threshold as the worst possible lag without any statistic needing to know
 * about it. It is never formatted; a cell checks for it and names the state.
 */
export const UNREADABLE = Infinity;

/** `null` is no reading at all, which is not the same as one that failed. */
const currentFrom = (latest: number | null | undefined) =>
  latest === null ? UNREADABLE : (latest ?? null);

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
      breachValue: statFor(stats, predicate),
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
 * A row is drawn on the graph when it breaches. Nothing else puts it there.
 */
export function buildFreshnessRows({
  judged,
  statsByKey,
  objectsById,
  hydrationByObjectId,
  threshold,
}: {
  judged: ThresholdLineSeries<DataPoint>[];
  statsByKey: Map<string, ObjectStats>;
  objectsById: Map<string, FreshnessObject>;
  hydrationByObjectId: Map<string, HydrationCounts>;
  threshold: number;
}): FreshnessRow[] {
  const colors = assignLineColors(judged);

  return judged
    .map((line) => {
      const stats = statsByKey.get(line.key) ?? {
        current: null,
        peak: null,
        p90: null,
      };
      const object = objectsById.get(line.key);
      const hydration = hydrationByObjectId.get(line.key);
      // Inclusive, matching `isBreaching` on the graph and the Objects page's
      // own threshold filter. A row and its line must agree.
      const breaching =
        line.breachValue !== null && line.breachValue >= threshold;

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
        color: breaching ? colors.get(line.key) : undefined,
      };
    })
    .sort(
      (a, b) =>
        (b.breachValue ?? -1) - (a.breachValue ?? -1) ||
        a.objectName.localeCompare(b.objectName),
    );
}
