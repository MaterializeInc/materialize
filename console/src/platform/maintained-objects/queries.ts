// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import {
  MAINTAINED_OBJECT_TYPES,
  MaintainedObjectType,
} from "~/api/materialize/maintained-objects/constants";
import {
  buildHydrationAggregateQuery,
  HydrationAggregateRow,
} from "~/api/materialize/maintained-objects/hydrationAggregate";
import {
  buildLagAggregateQuery,
  LagAggregateRow,
} from "~/api/materialize/maintained-objects/lagAggregate";
import {
  buildSubscribeQuery,
  useSubscribe,
} from "~/api/materialize/useSubscribe";
import { useAllObjects } from "~/store/allObjects";
import { sumPostgresIntervalMs } from "~/util";

export interface MaintainedObjectListItem {
  id: string;
  name: string;
  schemaName: string | null;
  databaseName: string | null;
  objectType: MaintainedObjectType;
  clusterId: string | null;
  clusterName: string | null;
  /** 0 until the hydration snapshot arrives. */
  hydratedReplicas: number;
  /** 0 until the hydration snapshot arrives. */
  totalReplicas: number;
  /** null until the lag snapshot arrives. */
  lag: LagAggregateRow["lag"];
  /** null until the lag snapshot arrives. */
  lagMs: number | null;
}

const MAINTAINED_OBJECT_TYPE_SET: ReadonlySet<string> = new Set(
  MAINTAINED_OBJECT_TYPES,
);

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
    const lagById = new Map(lag.data.map((r) => [r.object_id, r]));
    const hydrationById = new Map(hydration.data.map((r) => [r.object_id, r]));
    const num = (v: bigint | null | undefined) => (v ? Number(v) : 0);

    return allObjects.data
      .filter((obj) => {
        if (!obj.id.startsWith("u")) return false;
        if (!MAINTAINED_OBJECT_TYPE_SET.has(obj.objectType)) return false;
        // Subsources and progress sources are managed by their parent source.
        return !(
          obj.objectType === "source" &&
          (obj.sourceType === "subsource" || obj.sourceType === "progress")
        );
      })
      .map((obj) => {
        const lagRow = lagById.get(obj.id);
        const hydrationRow = hydrationById.get(obj.id);
        return {
          id: obj.id,
          name: obj.name,
          schemaName: obj.schemaName,
          databaseName: obj.databaseName,
          objectType: obj.objectType as MaintainedObjectType,
          clusterId: obj.clusterId,
          clusterName: obj.clusterName,
          hydratedReplicas: num(hydrationRow?.hydratedReplicas),
          totalReplicas: num(hydrationRow?.totalReplicas),
          lag: lagRow?.lag ?? null,
          lagMs: lagRow?.lag ? sumPostgresIntervalMs(lagRow.lag) : null,
        };
      })
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [allObjects.data, lag.data, hydration.data]);

  return {
    data,
    isLoading: !allObjects.snapshotComplete,
    lagReady: lag.snapshotComplete,
    hydrationReady: hydration.snapshotComplete,
    isError: allObjects.isError || lag.isError || hydration.isError,
  };
};
