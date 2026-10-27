// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { DataPoint, GraphLineSeries } from "~/components/FreshnessGraph/types";
import { FreshnessObject } from "~/platform/clusters/queries";

import {
  buildFreshnessRows,
  buildStats,
  FreshnessRow,
  judgeLines,
  Predicate,
} from "./freshnessRows";
import { HydrationCounts } from "./queries";

export interface FreshnessRowsParams {
  lines: GraphLineSeries[];
  historicalData: DataPoint[];
  latestByObjectId: Map<string, number | null>;
  objectsById: Map<string, FreshnessObject>;
  hydrationByObjectId: Map<string, HydrationCounts>;
  typeFilters: string[];
  predicate: Predicate;
  /** The settled threshold, not the one a drag is moving. */
  threshold: number;
}

/**
 * The rows and the judged lines behind the freshness page.
 *
 * A hook rather than five `useMemo`s in the page, so that a test can render it,
 * change one input, and assert what did and did not have to be rebuilt.
 *
 * That matters because a threshold drag re-renders on every pointer move, and
 * the page stays usable only while these caches hold. Each link depends on the
 * one above it, so a single input arriving with a new identity rebuilds a row
 * per object at pointer-move rate. Nothing about that failure is visible in
 * review: the code reads the same either way.
 */
export function useFreshnessRows({
  lines,
  historicalData,
  latestByObjectId,
  objectsById,
  hydrationByObjectId,
  typeFilters,
  predicate,
  threshold,
}: FreshnessRowsParams): {
  judged: ReturnType<typeof judgeLines>;
  rows: FreshnessRow[];
  breaching: FreshnessRow[];
} {
  const visibleLines = React.useMemo(
    () =>
      lines.filter((line) => {
        const object = objectsById.get(line.key);
        return (
          typeFilters.length === 0 ||
          (object !== undefined && typeFilters.includes(object.objectType))
        );
      }),
    [lines, objectsById, typeFilters],
  );

  const statsByKey = React.useMemo(
    () => buildStats(visibleLines, historicalData, latestByObjectId),
    [visibleLines, historicalData, latestByObjectId],
  );

  const judged = React.useMemo(
    () => judgeLines(visibleLines, historicalData, predicate, statsByKey),
    [visibleLines, historicalData, predicate, statsByKey],
  );

  const rows = React.useMemo(
    () =>
      buildFreshnessRows({
        judged,
        statsByKey,
        objectsById,
        hydrationByObjectId,
        threshold,
      }),
    [judged, statsByKey, objectsById, hydrationByObjectId, threshold],
  );

  const breaching = React.useMemo(
    () => rows.filter((row) => row.breaching),
    [rows],
  );

  return { judged, rows, breaching };
}
