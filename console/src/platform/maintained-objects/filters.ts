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

import { MaintainedObjectListItem } from "./queries";

/**
 * Row-threshold options for the freshness filter, keyed by seconds. Selecting
 * a value filters to objects whose pMAX lag meets the threshold.
 */
export const FRESHNESS_THRESHOLD_OPTIONS: Record<string, string> = {
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
  not_hydrated: "Not hydrated",
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

export const clusterFilterFn = arrayMatchFilter((r) => r.clusterName);

export const objectTypeFilterFn = arrayMatchFilter<MaintainedObjectType>(
  (r) => r.objectType,
);

export const hydrationFilterFn = arrayMatchFilter<HydrationBucket>((r) =>
  bucketForHydration(r.hydratedReplicas, r.totalReplicas),
);

export const freshnessFilterFn = (
  row: Row<MaintainedObjectListItem>,
  _id: string,
  thresholdSeconds: number,
) => {
  if (thresholdSeconds === undefined) return true;
  const lagMs = row.original.lagMs;
  return lagMs !== null && lagMs >= thresholdSeconds * 1_000;
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
      return v && v in FRESHNESS_THRESHOLD_OPTIONS
        ? parseInt(v, 10)
        : undefined;
    },
    toUrl: (v) =>
      typeof v === "number" ? { key: "freshness", value: v } : undefined,
  },
  {
    columnId: "hydration",
    fromUrl: (p) =>
      nonEmpty(
        p
          .getAll("hydration[]")
          .filter((h): h is HydrationBucket =>
            (HYDRATION_BUCKETS as readonly string[]).includes(h),
          ),
      ),
    toUrl: (v) => {
      const arr = v as HydrationBucket[];
      return arr?.length ? { key: "hydration[]", value: arr } : undefined;
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
