// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQuery } from "@tanstack/react-query";
import React from "react";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { IPostgresInterval, isSystemId } from "~/api/materialize";
import { fetchLagHistory } from "~/api/materialize/freshness/lagHistory";
import {
  MAINTAINED_OBJECT_TYPES,
  MaintainedObjectType,
} from "~/api/materialize/maintained-objects/constants";
import {
  CriticalPathRow,
  fetchCriticalPathAtTime,
  fetchCriticalPathPMaxInWindow,
} from "~/api/materialize/maintained-objects/criticalPath";
import {
  buildHydrationAggregateQuery,
  HydrationAggregateRow,
} from "~/api/materialize/maintained-objects/hydrationAggregate";
import {
  buildLagAggregateQuery,
  LagAggregateRow,
} from "~/api/materialize/maintained-objects/lagAggregate";
import { fetchSourceStatistics } from "~/api/materialize/source/sourceStatistics";
import {
  buildSubscribeQuery,
  useSubscribe,
} from "~/api/materialize/useSubscribe";
import { sourceQueryKeys } from "~/platform/sources/queries";
import { useAllObjects } from "~/store/allObjects";
import { sumPostgresIntervalMs } from "~/util";

import { buildObjectFreshnessHistory } from "./freshnessHistory";

/** Cluster the object is maintained on. `null` for tables, which aren't
 *  bound to a cluster. */
export interface MaintainedObjectCluster {
  id: string;
  name: string;
}

/** pMAX lag snapshot. `null` until the SUBSCRIBE delivers a row. */
export interface MaintainedObjectLag {
  /** Raw Postgres interval, used for human-readable formatting. */
  value: NonNullable<LagAggregateRow["lag"]>;
  /** Same value flattened to milliseconds, used for filters and sorts. */
  ms: number;
}

export interface MaintainedObjectListItem {
  id: string;
  name: string;
  schemaName: string;
  databaseName: string;
  objectType: MaintainedObjectType;
  /** Subtype for sources (e.g. `postgres`, `kafka`); null for non-sources. */
  sourceType: string | null;
  cluster: MaintainedObjectCluster | null;
  /** 0 until the hydration snapshot arrives. */
  hydratedReplicas: number;
  /** 0 until the hydration snapshot arrives. */
  totalReplicas: number;
  /** Null until the lag snapshot arrives. */
  lag: MaintainedObjectLag | null;
}

const MAINTAINED_OBJECT_TYPE_SET: ReadonlySet<string> = new Set(
  MAINTAINED_OBJECT_TYPES,
);

const bigintToNumber = (v: bigint | null | undefined): number =>
  v ? Number(v) : 0;

const useSubscribeToLagAggregate = ({
  lookbackMinutes,
}: {
  lookbackMinutes: number;
}) => {
  const subscribe = React.useMemo(
    () =>
      buildSubscribeQuery(buildLagAggregateQuery({ lookbackMinutes }), {
        upsertKey: "object_id",
      }),
    [lookbackMinutes],
  );
  return useSubscribe<LagAggregateRow, LagAggregateRow>({
    subscribe,
    upsertKey: (row) => row.data.object_id,
    select: (row) => row.data,
  });
};

const useSubscribeToHydrationAggregate = () => {
  const subscribe = React.useMemo(
    () =>
      buildSubscribeQuery(buildHydrationAggregateQuery(), {
        upsertKey: "object_id",
      }),
    [],
  );
  return useSubscribe<HydrationAggregateRow, HydrationAggregateRow>({
    subscribe,
    upsertKey: (row) => row.data.object_id,
    select: (row) => row.data,
  });
};

export interface UseMaintainedObjectsListResult {
  data: MaintainedObjectListItem[];
  /** True until the all-objects snapshot completes. */
  isLoading: boolean;
  /** Per-source snapshot flags; cells use these to show loading skeletons. */
  lagReady: boolean;
  hydrationReady: boolean;
  isError: boolean;
}

/**
 * Composes maintained objects from three live sources: `useAllObjects()` for
 * object metadata, plus SUBSCRIBEs for pMAX lag and hydration aggregate. The
 * table renders as soon as object metadata is ready; lag and hydration cells
 * fill in as their snapshots arrive.
 */
export const useMaintainedObjectsList = ({
  lookbackMinutes,
}: {
  lookbackMinutes: number;
}): UseMaintainedObjectsListResult => {
  const allObjects = useAllObjects();
  const lag = useSubscribeToLagAggregate({ lookbackMinutes });
  const hydration = useSubscribeToHydrationAggregate();

  const data = React.useMemo<MaintainedObjectListItem[]>(() => {
    const lagById = new Map(lag.data.map((r) => [r.object_id, r.lag]));
    const hydrationById = new Map(hydration.data.map((r) => [r.object_id, r]));

    const objectsById = new Map<string, MaintainedObjectListItem>();
    for (const obj of allObjects.data) {
      if (isSystemId(obj.id)) continue;
      if (!MAINTAINED_OBJECT_TYPE_SET.has(obj.objectType)) continue;
      // TODO(@leedqin): drop once `force_source_table_syntax` is on by
      // default (#30483); until then mz_objects still returns subsources as
      // sources, and we hide them and progress alongside their parent.
      if (obj.sourceType === "subsource" || obj.sourceType === "progress") {
        continue;
      }
      if (!obj.schemaName || !obj.databaseName) continue;
      const lagInterval = lagById.get(obj.id);
      const hydrationRow = hydrationById.get(obj.id);
      objectsById.set(obj.id, {
        id: obj.id,
        name: obj.name,
        schemaName: obj.schemaName,
        databaseName: obj.databaseName,
        objectType: obj.objectType as MaintainedObjectType,
        sourceType: obj.sourceType,
        cluster:
          obj.clusterId && obj.clusterName
            ? { id: obj.clusterId, name: obj.clusterName }
            : null,
        hydratedReplicas: bigintToNumber(hydrationRow?.hydratedReplicas),
        totalReplicas: bigintToNumber(hydrationRow?.totalReplicas),
        lag: lagInterval
          ? { value: lagInterval, ms: sumPostgresIntervalMs(lagInterval) }
          : null,
      });
    }
    return [...objectsById.values()].sort((a, b) =>
      a.name.localeCompare(b.name),
    );
  }, [allObjects.data, lag.data, hydration.data]);

  return {
    data,
    isLoading: !allObjects.snapshotComplete,
    lagReady: lag.snapshotComplete,
    hydrationReady: hydration.snapshotComplete,
    isError: allObjects.isError || lag.isError || hydration.isError,
  };
};

export interface ObjectSourceStatistics {
  messagesReceived: number;
  snapshotRecordsKnown: number;
  snapshotRecordsStaged: number;
  rehydrationLatency: IPostgresInterval | null;
}

export function useObjectSourceStatistics(sourceId: string) {
  return useQuery({
    queryKey: sourceQueryKeys.statistics({ sourceId }),
    queryFn: ({ queryKey, signal }) =>
      fetchSourceStatistics(queryKey, { sourceId }, { signal }),
    refetchInterval: 5_000,
    select: (data): ObjectSourceStatistics | null => {
      const row = data.rows.at(0);
      if (!row) return null;
      return {
        messagesReceived: Number(row.messagesReceived ?? 0),
        snapshotRecordsKnown: Number(row.snapshotRecordsKnown ?? 0),
        snapshotRecordsStaged: Number(row.snapshotRecordsStaged ?? 0),
        rehydrationLatency: row.rehydrationLatency as IPostgresInterval | null,
      };
    },
  });
}

const freshnessHistoryQueryKey = (objectId: string, lookbackMs: number) =>
  [
    ...buildRegionQueryKey("maintainedObjects"),
    buildQueryKeyPart("freshnessHistory", { objectId, lookbackMs }),
  ] as const;

export function useObjectFreshnessHistory({
  objectId,
  lookbackMs,
}: {
  objectId: string | undefined;
  lookbackMs: number;
}) {
  return useQuery({
    queryKey: freshnessHistoryQueryKey(objectId ?? "", lookbackMs),
    queryFn: async ({ queryKey, signal }) => {
      const { rows } = await fetchLagHistory({
        params: {
          lookback: { type: "historical", lookbackMs },
          objectIds: [objectId!],
        },
        requestOptions: { signal },
        queryKey,
      });
      return buildObjectFreshnessHistory(rows, lookbackMs);
    },
    enabled: !!objectId,
    staleTime: 30_000,
    refetchInterval: 60_000,
  });
}

export interface CriticalPathData {
  rows: CriticalPathRow[];
  /** Immediate direct upstream inputs for the object. */
  directInputs: CriticalPathRow[];
}

type CriticalPathMode =
  | { kind: "live"; lookbackMinutes: number }
  | { kind: "atTime"; timestamp: Date; bucketSizeMs: number };

const criticalPathQueryKey = (objectId: string, mode: CriticalPathMode) =>
  [
    ...buildRegionQueryKey("maintainedObjects"),
    buildQueryKeyPart("criticalPath", {
      objectId,
      mode:
        mode.kind === "live"
          ? { kind: "live", lookbackMinutes: mode.lookbackMinutes }
          : {
              kind: "atTime",
              timestamp: mode.timestamp.toISOString(),
              bucketSizeMs: mode.bucketSizeMs,
            },
    }),
  ] as const;

export function useCriticalPath({
  objectId,
  timestamp,
  bucketSizeMs,
  lookbackMinutes,
}: {
  objectId: string | undefined;
  timestamp: Date | null;
  bucketSizeMs: number;
  lookbackMinutes: number;
}) {
  const isLive = timestamp === null;
  const mode: CriticalPathMode = isLive
    ? { kind: "live", lookbackMinutes }
    : { kind: "atTime", timestamp, bucketSizeMs };
  return useQuery({
    queryKey: criticalPathQueryKey(objectId ?? "", mode),
    queryFn: ({ queryKey, signal }) =>
      fetchCriticalPath({
        objectId: objectId!,
        mode,
        signal,
        queryKey,
      }),
    enabled: !!objectId,
    staleTime: isLive ? 30_000 : Infinity,
    refetchInterval: isLive ? 30_000 : false,
  });
}

const fetchCriticalPath = async ({
  objectId,
  mode,
  signal,
  queryKey,
}: {
  objectId: string;
  mode: CriticalPathMode;
  signal: AbortSignal | undefined;
  queryKey: readonly unknown[];
}): Promise<CriticalPathData> => {
  const result =
    mode.kind === "live"
      ? await fetchCriticalPathPMaxInWindow({
          objectId,
          lookbackMinutes: mode.lookbackMinutes,
          queryKey,
          requestOptions: { signal },
        })
      : await fetchCriticalPathAtTime({
          objectId,
          timestamp: mode.timestamp,
          bucketSizeMs: mode.bucketSizeMs,
          queryKey,
          requestOptions: { signal },
        });

  const rows = result.rows;
  const directInputs = rows.filter(
    (r) => r.childId === objectId && r.id !== objectId,
  );
  return { rows, directInputs };
};
