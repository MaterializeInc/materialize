// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Row } from "@tanstack/react-table";

import {
  HYDRATION_BUCKETS,
  HYDRATION_LABELS,
  HydrationBucket,
} from "~/platform/maintained-objects/filters";

/** The hydration column's id, shared by the column, its chips, and the URL. */
export const HYDRATION_COLUMN_ID = "hydration";

/**
 * The URL parameter carrying the hydration filter, repeated once per selected
 * bucket. Named for this table's column rather than after the Maintained
 * Objects list's `status[]`, which covers source ingestion status as well as
 * hydration and so does not mean the same thing.
 */
export const HYDRATION_URL_KEY = "hydration[]";

/**
 * Keeps rows whose hydration bucket is one of `selected`. An empty selection
 * filters nothing.
 *
 * Reads the bucket off the column, whose accessor already returns it, so the
 * filter cannot select a value the cell does not show.
 *
 * NOTE: a replica whose hydration is unknown is dropped by any active
 * selection. It cannot be placed in a bucket, so a filtered list leaves it out
 * rather than guessing, matching how `utilizationFilterFn` treats a replica
 * with no reading.
 */
export const hydrationFilterFn = <TData>(
  row: Row<TData>,
  columnId: string,
  selected: HydrationBucket[],
) => {
  if (!selected?.length) return true;

  const bucket = row.getValue<HydrationBucket | undefined>(columnId);
  return bucket !== undefined && selected.includes(bucket);
};

/**
 * The buckets a URL asks for, or undefined when it names none that exist. A
 * hand-edited or stale link must leave the table unfiltered rather than install
 * a filter the panel cannot show or clear.
 *
 * Selecting from `HYDRATION_BUCKETS` rather than filtering the raw values drops
 * unknown names and repeats, and fixes the order, so the same set of buckets
 * always produces the same URL.
 */
export const hydrationFilterFromUrl = (raw: string[]) => {
  const buckets = HYDRATION_BUCKETS.filter((bucket) => raw.includes(bucket));
  return buckets.length ? buckets : undefined;
};

/**
 * One selected bucket stated for display, for example `Hydration: Hydrating`.
 * Names the column as well as the value, since the bucket labels alone do not
 * say what they describe.
 */
export const hydrationFilterLabel = (bucket: HydrationBucket) =>
  `Hydration: ${HYDRATION_LABELS[bucket]}`;
