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
export const SCOPE_SEARCH_PARAM = "scope";
export const CLUSTER_SEARCH_PARAM = "cluster";
export const SCHEMA_SEARCH_PARAM = "schema";
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

/** What the page gathers objects by: where they run, or where they are named. */
export type FreshnessScope = "cluster" | "schema";

export const SCOPE_LABELS: Record<
  FreshnessScope,
  { menu: string; noun: string; plural: string; place: string }
> = {
  cluster: {
    menu: "Cluster",
    noun: "cluster",
    plural: "clusters",
    place: "on this cluster",
  },
  schema: {
    menu: "Schema",
    noun: "schema",
    plural: "schemas",
    place: "in this schema",
  },
};

const SCHEMA_OBJECT_TYPE_FILTERS = [
  { label: "Sources", value: "source" },
  { label: "Tables", value: "table" },
  { label: "Materialized views", value: "materialized-view" },
  { label: "Indexes", value: "index" },
  { label: "Sinks", value: "sink" },
];

/**
 * The object types each scope offers to filter by, which are also the only
 * types a schema scope gathers: views, secrets and connections report no lag.
 *
 * Tables appear only by schema. A table carries a NULL `cluster_id`, so the
 * cluster-scoped lookup never reaches one, including a table created
 * `FROM SOURCE`, which is ingested on its source's cluster and does report lag.
 *
 * TODO: Reach those tables through their source rather than through
 * `cluster_id`. Cluster Overview has the same gap.
 *
 * Selecting nothing means every type, so there is no "All" option to pick.
 */
export const OBJECT_TYPE_FILTERS: Record<
  FreshnessScope,
  { label: string; value: string }[]
> = {
  cluster: SCHEMA_OBJECT_TYPE_FILTERS.filter(
    (filter) => filter.value !== "table",
  ),
  schema: SCHEMA_OBJECT_TYPE_FILTERS,
};

/** Shown where a statistic has no value, matching the rest of the Console. */
export const NO_VALUE = "—";
