// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Row } from "@tanstack/react-table";

/**
 * Keeps rows reporting at least `percent` of their allocation. Written for the
 * columns whose accessor returns a fraction, which the control states as a
 * percentage.
 *
 * The comparison is fixed at "at least": a utilization filter is asked for to
 * find what is running hot, so the threshold is a floor and needs no operator
 * alongside it.
 *
 * NOTE: compares the unrounded reading, the same value `PercentBar` colours a
 * bar by, so a row displaying "80.0%" can fall short of 80 when the reading
 * behind it is 0.7996.
 */
export const utilizationFilterFn = <TData>(
  row: Row<TData>,
  columnId: string,
  percent: number,
) => {
  const fraction = row.getValue<number | null | undefined>(columnId);
  // A replica with no sample in the window cannot be said to have reached a
  // threshold, so a filtered list leaves it out rather than guessing.
  if (fraction === null || fraction === undefined) return false;

  return fraction * 100 >= percent;
};

/** A threshold as its URL parameter value, for example `80`. */
export const utilizationFilterToUrl = (percent: number) => String(percent);

/**
 * The threshold a URL parameter asks for, or undefined when it is absent,
 * malformed, or not a positive percentage. A hand-edited or stale link must
 * leave the table unfiltered rather than install a filter the panel cannot show
 * or clear.
 */
export const utilizationFilterFromUrl = (raw: string | null) => {
  if (raw === null || !/^\d+(?:\.\d+)?$/.test(raw)) return undefined;
  const percent = parseFloat(raw);
  // Zero is every sampled replica, which is not a filter worth holding.
  return percent > 0 ? percent : undefined;
};

/**
 * A threshold stated for display, for example `CPU ≥ 40%`. Reads as the
 * condition it applies, so a chip or a summary needs nothing added around it.
 */
export const utilizationFilterLabel = (heading: string, percent: number) =>
  `${heading} ≥ ${percent}%`;
