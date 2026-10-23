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

import { HydrationCounts, LagPercentiles } from "./queries";

/** Which statistic the threshold judges an object by. */
export type Predicate = "current" | "p90" | "p99" | "peak";

/**
 * How each predicate is written, in the two places it appears.
 *
 * `menu` is the statistic's bare name, read inside "had a [p99] freshness".
 * `sentence` reads as prose in the message shown when nothing exceeds the
 * threshold.
 */
export const PREDICATE_LABELS: Record<
  Predicate,
  { menu: string; sentence: string }
> = {
  current: { menu: "current", sentence: "right now" },
  p90: {
    menu: "p90",
    sentence: "for more than 10% of minutes",
  },
  p99: {
    menu: "p99",
    sentence: "for more than 1% of minutes",
  },
  peak: { menu: "pMAX", sentence: "at any moment" },
};

/**
 * The clause the "No objects exceeded" message ends with.
 *
 * "Right now" takes no range: it describes this moment whatever is graphed.
 */
export function describeThreshold(
  threshold: string,
  predicate: Predicate,
  rangeLabel: string,
): string {
  const suffix =
    predicate === "current" ? "" : ` over the last ${rangeLabel.toLowerCase()}`;
  return `${threshold} ${PREDICATE_LABELS[predicate].sentence}${suffix}`;
}

/**
 * A schema named with its database, because every database has its own
 * `public` and the bare names are indistinguishable.
 */
export function qualifiedSchemaName(
  databaseName: string | null,
  schemaName: string,
) {
  return databaseName ? `${databaseName}.${schemaName}` : schemaName;
}

export interface FreshnessRow {
  key: string;
  objectName: string;
  databaseName: string | null;
  schemaId: string | null;
  schemaName: string | null;
  clusterId: string | null;
  clusterName: string | null;
  objectType: string;
  hydratedReplicas: number;
  totalReplicas: number;
  current: number | null;
  peak: number | null;
  p90: number | null;
  p99: number | null;
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
  p99: number | null;
  p99_9: number | null;
}

/**
 * The statistics a predicate can judge, from the object's newest reading and
 * its percentiles over the range.
 *
 * `percentiles` is undefined for an object with no readings in the range, and
 * its percentiles are then null rather than `UNREADABLE`: no reading says
 * nothing about the object.
 */
export function computeStats(
  latest: number | null | undefined,
  percentiles: LagPercentiles | undefined,
): ObjectStats {
  const current = currentFrom(latest);

  if (!percentiles) {
    return { current, peak: current, p90: null, p99: null, p99_9: null };
  }

  return {
    current,
    // The percentiles refetch once per bin and the latest reading once a
    // minute, so a lag that starts between percentile fetches is only in
    // `current`.
    peak: Math.max(percentiles.peak, current ?? -Infinity),
    p90: percentiles.p90,
    p99: percentiles.p99,
    p99_9: percentiles.p99_9,
  };
}

/**
 * A reading whose lag came back NULL: the object could not be read.
 *
 * Sorts, ranks and compares against a threshold as the worst possible lag, so
 * no statistic needs to know about it. Never formatted: a cell checks for it
 * and names the state.
 */
export const UNREADABLE = Infinity;

/** `null` is no reading at all, which is not the same as one that failed. */
const currentFrom = (latest: number | null | undefined) =>
  latest === null ? UNREADABLE : (latest ?? null);

/**
 * The worst newest minute among `objectIds`, or undefined when none has one.
 * An unreadable minute counts as the worst, as in `computeStats`.
 */
export function latestPeak(
  objectIds: string[],
  latestByObjectId: Map<string, number | null> | undefined,
): number | undefined {
  let peak: number | undefined;
  for (const id of objectIds) {
    const current = currentFrom(latestByObjectId?.get(id));
    if (current !== null && (peak === undefined || current > peak)) {
      peak = current;
    }
  }
  return peak;
}

const statFor = (stats: ObjectStats, predicate: Predicate) => stats[predicate];

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
  predicate: Predicate,
  statsByKey: Map<string, ObjectStats>,
): ThresholdLineSeries<DataPoint>[] {
  return lines.map((line) => {
    const stats =
      statsByKey.get(line.key) ?? computeStats(undefined, undefined);
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
  latestByObjectId: Map<string, number | null>,
  percentilesByObjectId: Map<string, LagPercentiles>,
): Map<string, ObjectStats> {
  return new Map(
    lines.map((line) => [
      line.key,
      computeStats(
        latestByObjectId.get(line.key),
        percentilesByObjectId.get(line.key),
      ),
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
      const stats =
        statsByKey.get(line.key) ?? computeStats(undefined, undefined);
      const object = objectsById.get(line.key);
      const hydration = hydrationByObjectId.get(line.key);
      // Inclusive, matching `isBreaching` on the graph and the Objects page's
      // own threshold filter. A row and its line must agree.
      const breaching =
        line.breachValue !== null && line.breachValue >= threshold;

      return {
        key: line.key,
        objectName: object?.objectName ?? line.label ?? line.key,
        databaseName: object?.databaseName ?? null,
        schemaId: object?.schemaId ?? null,
        schemaName: object?.schemaName ?? null,
        clusterId: object?.clusterId ?? null,
        clusterName: object?.clusterName ?? null,
        objectType: object?.objectType ?? "",
        hydratedReplicas: hydration?.hydratedReplicas ?? 0,
        totalReplicas: hydration?.totalReplicas ?? 0,
        current: stats.current,
        peak: stats.peak,
        p90: stats.p90,
        p99: stats.p99,
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
