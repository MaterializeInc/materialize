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
 * Sorting function that pushes `null` / `undefined` values to the end and
 * compares the rest with numeric-aware locale collation (so "25cc" sorts
 * before "100cc" instead of lexicographically).
 */
const nullsLast = <TData>(
  rowA: Row<TData>,
  rowB: Row<TData>,
  columnId: string,
): number => {
  const a = rowA.getValue(columnId);
  const b = rowB.getValue(columnId);

  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;

  return String(a).localeCompare(String(b), undefined, { numeric: true });
};

/**
 * Sorting function for columns whose values are real numbers, pushing `null` /
 * `undefined` to the end.
 *
 * `nullsLast` collates as text, which mis-orders fractions: numeric collation
 * treats "." as a separator, so it compares 12.48 as (12, 48) against 12.5 as
 * (12, 5) and reports 12.48 as the larger of the two.
 */
const numericNullsLast = <TData>(
  rowA: Row<TData>,
  rowB: Row<TData>,
  columnId: string,
): number => {
  const a = rowA.getValue<number | null | undefined>(columnId);
  const b = rowB.getValue<number | null | undefined>(columnId);

  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;

  return a - b;
};

export const sortingFunctions = {
  nullsLast,
  numericNullsLast,
};
