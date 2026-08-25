// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Row } from "@tanstack/react-table";

/** The side of the threshold a row has to fall on to be kept. */
export type UtilizationComparison = ">" | "<";

export interface UtilizationFilterValue {
  comparison: UtilizationComparison;
  /** Threshold as a whole percentage, as typed into the control. */
  percent: number;
}

export const DEFAULT_COMPARISON: UtilizationComparison = ">";

/**
 * Keeps rows whose utilization reading falls on the requested side of the
 * threshold. Written for the columns whose accessor returns a fraction of the
 * replica's allocation, which the control states as a percentage.
 *
 * NOTE: compares the unrounded reading, the same value `PercentBar` colours a
 * bar by, so a row displaying "80.0%" can fall outside "> 80" when the reading
 * behind it is 0.7996.
 */
export const utilizationFilterFn = <TData>(
  row: Row<TData>,
  columnId: string,
  filterValue: UtilizationFilterValue,
) => {
  const fraction = row.getValue<number | null | undefined>(columnId);
  // A replica with no sample in the window cannot be said to sit on either
  // side of a threshold, so a filtered list leaves it out rather than
  // guessing.
  if (fraction === null || fraction === undefined) return false;

  const percent = fraction * 100;
  return filterValue.comparison === ">"
    ? percent > filterValue.percent
    : percent < filterValue.percent;
};

/**
 * How a comparison is spelled in the URL. Words rather than the operators
 * themselves: `>` percent-encodes to `%3E`, which makes a bookmarked URL
 * unreadable.
 */
const COMPARISON_URL_TOKENS: Record<UtilizationComparison, string> = {
  ">": "gt",
  "<": "lt",
};

/**
 * A filter as one URL parameter value, for example `gt.80`. Anchored, so the
 * separator is unambiguous even when the threshold carries a decimal point.
 */
const URL_VALUE_PATTERN = /^(gt|lt)\.(\d+(?:\.\d+)?)$/;

export const utilizationFilterToUrl = (value: UtilizationFilterValue) =>
  `${COMPARISON_URL_TOKENS[value.comparison]}.${value.percent}`;

/**
 * The filter a URL parameter asks for, or undefined when it is absent or
 * malformed. A hand-edited or stale link must leave the table unfiltered rather
 * than install a filter the control cannot show or clear.
 */
export const utilizationFilterFromUrl = (
  raw: string | null,
): UtilizationFilterValue | undefined => {
  const match = raw?.match(URL_VALUE_PATTERN);
  if (!match) return undefined;
  return {
    comparison: match[1] === "gt" ? ">" : "<",
    percent: parseFloat(match[2]),
  };
};
