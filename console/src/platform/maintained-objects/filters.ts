// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ColumnFiltersState, Row } from "@tanstack/react-table";

import {
  MAINTAINED_OBJECT_TYPES,
  MaintainedObjectType,
} from "~/api/materialize/maintained-objects/constants";
import { snapshotting } from "~/platform/connectors/utils";

import {
  MaintainedObjectListItem,
  MaintainedObjectSourceStatus,
} from "./queries";

/**
 * Row-threshold options for the freshness filter, keyed by seconds. Selecting
 * a value filters to objects whose pMAX lag meets the threshold.
 */
export const FRESHNESS_THRESHOLD_OPTIONS: Record<string, string> = {
  "5": "pMAX ≥ 5 seconds",
  "10": "pMAX ≥ 10 seconds",
  "60": "pMAX ≥ 1 minute",
  "300": "pMAX ≥ 5 minutes",
  "900": "pMAX ≥ 15 minutes",
  "1800": "pMAX ≥ 30 minutes",
  "3600": "pMAX ≥ 1 hour",
};

export const HYDRATION_BUCKETS = [
  "hydrated",
  "hydrating",
  "not_hydrated",
] as const;
export type HydrationBucket = (typeof HYDRATION_BUCKETS)[number];

export const HYDRATION_LABELS: Record<HydrationBucket, string> = {
  hydrated: "Hydrated",
  hydrating: "Hydrating",
  not_hydrated: "Not Hydrated",
};

export const STATUS_COLOR_SCHEMES: Record<
  HydrationBucket,
  "green" | "yellow" | "red"
> = {
  hydrated: "green",
  hydrating: "yellow",
  not_hydrated: "red",
};

/** Bucketize per-object replica counts. Returns undefined when totals are
 *  unknown (no rows in `mz_hydration_statuses` for the object yet). */
export const bucketForHydration = (
  hydratedReplicas: number,
  totalReplicas: number,
): HydrationBucket | undefined => {
  if (totalReplicas === 0) return undefined;
  if (hydratedReplicas === 0) return "not_hydrated";
  if (hydratedReplicas === totalReplicas) return "hydrated";
  return "hydrating";
};

/** Filter rows where `extract(row)` is one of the selected values. */
export const arrayMatchFilter =
  <T>(extract: (row: MaintainedObjectListItem) => T | null | undefined) =>
  (row: Row<MaintainedObjectListItem>, _id: string, selected: T[]) => {
    if (!selected?.length) return true;
    const value = extract(row.original);
    return value != null && selected.includes(value);
  };

export const clusterFilterFn = arrayMatchFilter((r) => r.cluster?.name);

export const objectTypeFilterFn = arrayMatchFilter<MaintainedObjectType>(
  (r) => r.objectType,
);

/**
 * Status values offered for sources. `snapshotting` is derived rather than
 * reported: `mz_source_statuses` says `running` throughout the initial
 * snapshot, so both the pill and this filter fold in `snapshot_committed`.
 */
export const SOURCE_STATUS_BUCKETS = [
  "snapshotting",
  "running",
  "starting",
  "created",
  "paused",
  "stalled",
  "failed",
] as const;
export type SourceStatusBucket = (typeof SOURCE_STATUS_BUCKETS)[number];

export const SOURCE_STATUS_LABELS: Record<SourceStatusBucket, string> = {
  snapshotting: "Snapshotting",
  running: "Running",
  starting: "Starting",
  created: "Created",
  paused: "Paused",
  stalled: "Stalled",
  failed: "Failed",
};

/** Derives a source's displayed status, matching `ConnectorStatusPill`. */
export const bucketForSourceStatus = (
  sourceStatus: MaintainedObjectSourceStatus,
  sourceType: string | null,
): string =>
  snapshotting({
    status: sourceStatus.status,
    type: sourceType ?? "",
    snapshotCommitted: sourceStatus.snapshotCommitted,
  })
    ? "snapshotting"
    : sourceStatus.status;

/** Options the Status filter offers, sources first. */
export const STATUS_FILTER_BUCKETS = [
  ...SOURCE_STATUS_BUCKETS,
  ...HYDRATION_BUCKETS,
] as const;

export const STATUS_FILTER_LABELS: Record<string, string> = {
  ...SOURCE_STATUS_LABELS,
  ...HYDRATION_LABELS,
};

/**
 * The bucket the Status cell displays: ingestion status for sources, replica
 * hydration for everything else. The filter matches on this so it can never
 * select a value the cell doesn't show.
 */
export const statusBucketForRow = (
  row: MaintainedObjectListItem,
): string | undefined => {
  if (row.objectType === "source") {
    return row.sourceStatus
      ? bucketForSourceStatus(row.sourceStatus, row.sourceType)
      : undefined;
  }
  return bucketForHydration(row.hydratedReplicas, row.totalReplicas);
};

export const statusFilterFn = arrayMatchFilter<string>(statusBucketForRow);

export const freshnessFilterFn = (
  row: Row<MaintainedObjectListItem>,
  _id: string,
  thresholdSeconds: number,
) => {
  if (thresholdSeconds === undefined) return true;
  const lagMs = row.original.lag?.ms;
  return lagMs !== undefined && lagMs >= thresholdSeconds * 1_000;
};

/**
 * Maps a TanStack column filter to and from the URL search params. Array URL
 * keys carry their `[]` suffix literally (see `useSyncObjectToSearchParams`).
 */
export interface FilterUrlSpec {
  columnId: string;
  /** Read this filter's value from URL params; undefined = not set. */
  fromUrl(params: URLSearchParams): unknown;
  /** Encode this filter's value for the URL object; undefined = skip. */
  toUrl(value: unknown): { key: string; value: unknown } | undefined;
}

const nonEmpty = <T>(arr: T[]): T[] | undefined =>
  arr.length > 0 ? arr : undefined;

export const FILTER_URL_SPECS: readonly FilterUrlSpec[] = [
  {
    columnId: "clusterName",
    fromUrl: (p) => nonEmpty(p.getAll("clusters[]")),
    toUrl: (v) => {
      const arr = v as string[];
      return arr?.length ? { key: "clusters[]", value: arr } : undefined;
    },
  },
  {
    columnId: "objectType",
    fromUrl: (p) =>
      nonEmpty(
        p
          .getAll("objectType[]")
          .filter((t): t is MaintainedObjectType =>
            (MAINTAINED_OBJECT_TYPES as readonly string[]).includes(t),
          ),
      ),
    toUrl: (v) => {
      const arr = v as MaintainedObjectType[];
      return arr?.length ? { key: "objectType[]", value: arr } : undefined;
    },
  },
  {
    columnId: "freshness",
    fromUrl: (p) => {
      const v = p.get("freshness");
      if (!v) return undefined;
      const n = parseInt(v, 10);
      return Number.isFinite(n) && n > 0 ? n : undefined;
    },
    toUrl: (v) =>
      typeof v === "number" ? { key: "freshness", value: v } : undefined,
  },
  {
    // Matches the Status column's id. The filter, its URL param and its chip
    // all have to agree on this key or the filter silently stops round-tripping.
    columnId: "status",
    fromUrl: (p) =>
      nonEmpty(
        p
          .getAll("status[]")
          .filter((s) =>
            (STATUS_FILTER_BUCKETS as readonly string[]).includes(s),
          ),
      ),
    toUrl: (v) => {
      const arr = v as string[];
      return arr?.length ? { key: "status[]", value: arr } : undefined;
    },
  },
];

export const initialColumnFiltersFromUrl = (
  search: string,
): ColumnFiltersState => {
  const params = new URLSearchParams(search);
  return FILTER_URL_SPECS.flatMap((spec) => {
    const value = spec.fromUrl(params);
    return value === undefined ? [] : [{ id: spec.columnId, value }];
  });
};
