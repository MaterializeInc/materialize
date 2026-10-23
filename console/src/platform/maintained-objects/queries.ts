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
import {
  Bucket,
  fetchReplicaUtilizationHistory,
} from "~/api/materialize/cluster/replicaUtilizationHistory";
import { fetchObjectLagHistory } from "~/api/materialize/freshness/lagHistory";
import {
  MAINTAINED_OBJECT_TYPES,
  MaintainedObjectListType,
} from "~/api/materialize/maintained-objects/constants";
import {
  CriticalPathRow,
  fetchCriticalPathAtTime,
  fetchCriticalPathLive,
} from "~/api/materialize/maintained-objects/criticalPath";
import {
  buildHydrationAggregateQuery,
  HydrationAggregateRow,
} from "~/api/materialize/maintained-objects/hydrationAggregate";
import {
  buildLagAggregateQuery,
  LagAggregateRow,
} from "~/api/materialize/maintained-objects/lagAggregate";
import { DatabaseObject } from "~/api/materialize/objects";
import { fetchSourceStatistics } from "~/api/materialize/source/sourceStatistics";
import {
  buildSubscribeQuery,
  useSubscribe,
} from "~/api/materialize/useSubscribe";
import { sourceQueryKeys } from "~/platform/sources/queries";
import { useAllObjects } from "~/store/allObjects";
import { sumPostgresIntervalMs } from "~/util";

import {
  LagHistoryRow,
  transformObjectFreshnessHistory,
} from "./freshnessHistory";

/** Cluster the object is maintained on. */
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

/**
 * Ingestion state from the hydration-aggregate feed, which joins
 * `mz_source_statuses` and the snapshot flag. This is what the Status column
 * shows for a source, in place of replica hydration, which for a source only
 * tracks whether `rehydration_latency` is set and so reads false throughout the
 * initial snapshot.
 */
export interface MaintainedObjectSourceStatus {
  /** One of `created`, `starting`, `running`, `paused`, `stalled`, `failed`. */
  status: string;
  /** Populated when the source is `stalled` or `failed`. */
  error: string | null;
  /** Null while no replica has reported statistics yet. */
  snapshotCommitted: boolean | null;
}

export interface MaintainedObjectListItem {
  id: string;
  name: string;
  schemaName: string;
  databaseName: string;
  objectType: MaintainedObjectListType;
  /** Connector type (e.g. `postgres`, `kafka`) of a source, or of the source
   *  ingesting a subsource or table. Null for everything else. */
  sourceType: string | null;
  /** The source ingesting a subsource or table. Null for everything else,
   *  and until the hydration snapshot arrives. */
  parentSource: { id: string; name: string } | null;
  /** For a subsource or table, the cluster its source ingests on. */
  cluster: MaintainedObjectCluster | null;
  /** 0 until the hydration snapshot arrives. */
  hydratedReplicas: number;
  /** 0 until the hydration snapshot arrives. */
  totalReplicas: number;
  /** Null until the lag snapshot arrives. */
  lag: MaintainedObjectLag | null;
  /** Null for non-sources, and until the hydration snapshot arrives. */
  sourceStatus: MaintainedObjectSourceStatus | null;
}

/**
 * True for a source and for the subsources and tables it ingests into. These
 * report ingestion status in place of replica hydration, and have no upstream
 * dataflow for the critical path to walk.
 */
export const isSourceCollection = (item: MaintainedObjectListItem) =>
  item.objectType === "source" ||
  item.objectType === "subsource" ||
  item.objectType === "table";

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

/**
 * Joins object metadata with the lag and hydration aggregates into the list's
 * rows, sorted by name.
 */
export const buildMaintainedObjectsList = ({
  objects,
  lagRows,
  hydrationRows,
}: {
  objects: DatabaseObject[];
  lagRows: LagAggregateRow[];
  hydrationRows: HydrationAggregateRow[];
}): MaintainedObjectListItem[] => {
  const lagById = new Map(lagRows.map((r) => [r.object_id, r.lag]));
  const hydrationById = new Map(hydrationRows.map((r) => [r.object_id, r]));
  const metadataById = new Map(objects.map((o) => [o.id, o]));

  const objectsById = new Map<string, MaintainedObjectListItem>();
  for (const obj of objects) {
    if (isSystemId(obj.id)) continue;
    if (!obj.schemaName || !obj.databaseName) continue;
    const hydrationRow = hydrationById.get(obj.id);
    const parent = hydrationRow?.parentSourceId
      ? metadataById.get(hydrationRow.parentSourceId)
      : undefined;

    // `mz_objects` reports subsources and progress collections as sources.
    // A progress collection's freshness is its source's, so it is hidden.
    let objectType: MaintainedObjectListType;
    if (obj.sourceType === "progress") {
      continue;
    } else if (obj.sourceType === "subsource") {
      objectType = "subsource";
    } else if (obj.objectType === "table") {
      // Only source-fed tables are maintained. One is indistinguishable from
      // a table written with INSERT until the hydration snapshot names its
      // source.
      if (!parent) continue;
      objectType = "table";
    } else if (MAINTAINED_OBJECT_TYPE_SET.has(obj.objectType)) {
      objectType = obj.objectType as MaintainedObjectListType;
    } else {
      continue;
    }

    const clusterOwner = parent ?? obj;
    const lagInterval = lagById.get(obj.id);
    objectsById.set(obj.id, {
      id: obj.id,
      name: obj.name,
      schemaName: obj.schemaName,
      databaseName: obj.databaseName,
      objectType,
      sourceType: parent ? parent.sourceType : obj.sourceType,
      parentSource: parent ? { id: parent.id, name: parent.name } : null,
      cluster:
        clusterOwner.clusterId && clusterOwner.clusterName
          ? { id: clusterOwner.clusterId, name: clusterOwner.clusterName }
          : null,
      hydratedReplicas: bigintToNumber(hydrationRow?.hydratedReplicas),
      totalReplicas: bigintToNumber(hydrationRow?.totalReplicas),
      lag: lagInterval
        ? { value: lagInterval, ms: sumPostgresIntervalMs(lagInterval) }
        : null,
      sourceStatus: hydrationRow?.sourceStatus
        ? {
            status: hydrationRow.sourceStatus,
            error: hydrationRow.sourceError,
            snapshotCommitted: hydrationRow.snapshotCommitted,
          }
        : null,
    });
  }
  return [...objectsById.values()].sort((a, b) => a.name.localeCompare(b.name));
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
 * object metadata, plus SUBSCRIBEs for pMAX lag and the hydration aggregate,
 * which also carries source status. The table renders as soon as object
 * metadata is ready; lag and status cells fill in as their snapshots arrive.
 */
export const useMaintainedObjectsList = ({
  lookbackMinutes,
}: {
  lookbackMinutes: number;
}): UseMaintainedObjectsListResult => {
  const allObjects = useAllObjects();
  const lag = useSubscribeToLagAggregate({ lookbackMinutes });
  const hydration = useSubscribeToHydrationAggregate();

  const data = React.useMemo(
    () =>
      buildMaintainedObjectsList({
        objects: allObjects.data,
        lagRows: lag.data,
        hydrationRows: hydration.data,
      }),
    [allObjects.data, lag.data, hydration.data],
  );

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
  object,
  lookbackMs,
}: {
  object: Pick<MaintainedObjectListItem, "id" | "name" | "schemaName">;
  lookbackMs: number;
}) {
  const { id: objectId, name, schemaName } = object;
  // NOTE: `fetchLagHistory` would return names, but it inner-joins
  // `mz_clusters`, so it returns nothing for subsources and source-fed tables.
  const select = React.useCallback(
    (rows: Omit<LagHistoryRow, "schemaName" | "objectName">[]) =>
      transformObjectFreshnessHistory(
        rows.map((r) => ({ ...r, schemaName, objectName: name })),
        lookbackMs,
      ),
    [name, schemaName, lookbackMs],
  );
  return useQuery({
    queryKey: freshnessHistoryQueryKey(objectId, lookbackMs),
    queryFn: async ({ queryKey, signal }) => {
      const { rows } = await fetchObjectLagHistory({
        objectIds: [objectId],
        lookbackMs,
        requestOptions: { signal },
        queryKey,
      });
      return rows;
    },
    select,
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
  | { kind: "live"; lookbackMinutes: number; bucketSizeMs: number }
  | {
      kind: "atTime";
      timestamp: Date;
      bucketSizeMs: number;
      lookbackMinutes: number;
    };

const criticalPathQueryKey = (objectId: string, mode: CriticalPathMode) =>
  [
    ...buildRegionQueryKey("maintainedObjects"),
    buildQueryKeyPart("criticalPath", {
      objectId,
      mode:
        mode.kind === "live"
          ? {
              kind: "live",
              lookbackMinutes: mode.lookbackMinutes,
              bucketSizeMs: mode.bucketSizeMs,
            }
          : {
              kind: "atTime",
              timestamp: mode.timestamp.toISOString(),
              bucketSizeMs: mode.bucketSizeMs,
              lookbackMinutes: mode.lookbackMinutes,
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
    ? { kind: "live", lookbackMinutes, bucketSizeMs }
    : { kind: "atTime", timestamp, bucketSizeMs, lookbackMinutes };
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
      ? await fetchCriticalPathLive({
          objectId,
          lookbackMinutes: mode.lookbackMinutes,
          bucketSizeMs: mode.bucketSizeMs,
          queryKey,
          requestOptions: { signal },
        })
      : await fetchCriticalPathAtTime({
          objectId,
          timestamp: mode.timestamp,
          bucketSizeMs: mode.bucketSizeMs,
          lookbackMinutes: mode.lookbackMinutes,
          queryKey,
          requestOptions: { signal },
        });

  const rows = result.rows;
  const directInputs = rows.filter(
    (r) => r.childId === objectId && r.id !== objectId,
  );
  return { rows, directInputs };
};

/** Binned lag readings for several objects, as the freshness chart bins them. */
export function useObjectsLagHistory({
  objectIds,
  lookbackMs,
}: {
  objectIds: string[];
  lookbackMs: number;
}) {
  return useQuery({
    queryKey: [
      ...buildRegionQueryKey("maintainedObjects"),
      buildQueryKeyPart("objectsLagHistory", { objectIds, lookbackMs }),
    ],
    queryFn: async ({ queryKey, signal }) => {
      const { rows } = await fetchObjectLagHistory({
        objectIds,
        lookbackMs,
        requestOptions: { signal },
        queryKey,
      });
      return rows;
    },
    enabled: objectIds.length > 0,
    staleTime: 30_000,
    refetchInterval: 60_000,
  });
}

export interface ReplicaInWindow {
  replicaId: string;
  name: string;
  size: string | null;
  lastSeenAt: Date;
}

/** Returns each replica's metric buckets in a `lookbackMs`-long window
 *  ending at `anchorTimestamp` (or now if `anchorTimestamp` is null).
 *  Polls every 30s when anchored to now; otherwise stays static. */
export const useClusterBucketsInWindow = ({
  clusterId,
  lookbackMs,
  bucketSizeMs,
  anchorTimestamp,
}: {
  clusterId: string | null;
  lookbackMs: number;
  bucketSizeMs: number;
  anchorTimestamp: Date | null;
}) => {
  const isLive = anchorTimestamp === null;
  const anchorMs = (anchorTimestamp ?? new Date()).getTime();
  const startMs =
    Math.floor((anchorMs - lookbackMs) / bucketSizeMs) * bucketSizeMs;

  return useQuery({
    queryKey: [
      ...buildRegionQueryKey("maintainedObjects"),
      buildQueryKeyPart("clusterBucketsInWindow", {
        clusterId: clusterId ?? "",
        startMs,
        bucketSizeMs,
      }),
    ],
    queryFn: async ({
      queryKey,
      signal,
    }): Promise<Record<string, Bucket[]>> => {
      if (clusterId === null) return {};
      const { bucketsByReplicaId } = await fetchReplicaUtilizationHistory({
        params: {
          clusterIds: [clusterId],
          startDate: new Date(startMs).toISOString(),
          bucketSizeMs,
        },
        queryKey,
        requestOptions: { signal },
      });
      return bucketsByReplicaId;
    },
    enabled: !!clusterId,
    staleTime: isLive ? 30_000 : Infinity,
    refetchInterval: isLive ? 30_000 : false,
  });
};
