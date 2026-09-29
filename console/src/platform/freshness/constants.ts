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

/** A tenth of a second: finer than the measurement, coarse enough to drag to. */
export const THRESHOLD_STEP_MS = 100;

/**
 * How long the threshold must sit still before the rest of the page follows it.
 *
 * The drag handle has to track the cursor every frame, but the work behind it
 * does not: writing the search param re-renders the route, and rebuilding the
 * table re-renders a row per object. Both at pointer-move rate is what made
 * dragging stutter. Short enough that a reader who stops moving sees the table
 * catch up as one motion rather than as a delay.
 */
export const THRESHOLD_SETTLE_MS = 120;

/**
 * Type filter chips.
 *
 * Tables are absent on purpose: a table has no cluster, so a cluster-scoped
 * page can never list one. `mz_relations` gives `'table'` a NULL `cluster_id`.
 */
export const OBJECT_TYPE_FILTERS: { label: string; value: string }[] = [
  { label: "Sources", value: "source" },
  { label: "Materialized views", value: "materialized-view" },
  { label: "Indexes", value: "index" },
  { label: "Sinks", value: "sink" },
];

/** Selecting nothing means every type, so there is no "All" option to pick. */
export const OBJECT_TYPE_VALUES = OBJECT_TYPE_FILTERS.map((f) => f.value);
