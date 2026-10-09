// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Freshness is wallclock lag: how far behind wall clock time an object's
 * results are. The whole page is denominated in it, so thresholds, axes and
 * table cells are all milliseconds.
 */

export const TIME_PERIOD_OPTIONS = {
  "60": "1 hour",
  "180": "3 hours",
  "360": "6 hours",
  "1440": "24 hours",
};

/**
 * Search params carrying the page's controls, so a view is linkable. The time
 * period reuses the app-wide `timePeriod` key from `useTimePeriodSelect`.
 */
export const CLUSTER_SEARCH_PARAM = "cluster";
export const THRESHOLD_SEARCH_PARAM = "threshold";
export const PREDICATE_SEARCH_PARAM = "exceeded";
export const OBJECT_TYPE_SEARCH_PARAM = "type";

export const DEFAULT_TIME_PERIOD_MINUTES = 60;

/**
 * Where the threshold starts before anyone moves it.
 *
 * Two seconds is a placeholder, not a recommendation: what counts as stale is
 * a property of what an object feeds, which nothing in the catalog knows. The
 * control exists so a reader can find their own line.
 */
export const DEFAULT_THRESHOLD_MS = 2_000;

/**
 * The object types this page offers to filter by.
 *
 * Tables are not among them. A table carries a NULL `cluster_id`, so the
 * cluster-scoped lookup this page does never reaches one, including a table
 * created `FROM SOURCE`, which is ingested on its source's cluster and does
 * report lag. Such a table is missing from this page today.
 *
 * TODO: Reach those tables through their source rather than through
 * `cluster_id`. Cluster Overview has the same gap.
 */
export const OBJECT_TYPE_FILTERS: { label: string; value: string }[] = [
  { label: "Sources", value: "source" },
  { label: "Materialized views", value: "materialized-view" },
  { label: "Indexes", value: "index" },
  { label: "Sinks", value: "sink" },
];

/** Selecting nothing means every type, so there is no "All" option to pick. */
export const OBJECT_TYPE_VALUES = OBJECT_TYPE_FILTERS.map((f) => f.value);
