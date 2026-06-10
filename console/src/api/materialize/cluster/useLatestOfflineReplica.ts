// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { isAfter, subMinutes } from "date-fns";
import { InferResult, sql } from "kysely";
import React from "react";

import { queryBuilder } from "../db";
import { useSubscribe } from "../useSubscribe";

type ClusterId = string;

export type LatestOfflineReplicaInfo = {
  shouldSurfaceOom: boolean;
  lastOfflineAt: Date;
};

export type LatestOfflineReplicaMap = Map<ClusterId, LatestOfflineReplicaInfo>;

export type QueryPayload = {
  mz_state: string;
  cluster_id: string;
  lastOffilneAt: Date;
  isOom: boolean;
};

function buildLatestOfflineReplicaQuery() {
  return queryBuilder
    .selectFrom("mz_cluster_replicas as cr")
    .leftJoin("mz_cluster_replica_statuses as crs", "cr.id", "crs.replica_id")
    .select([
      "crs.replica_id as replicaId",
      "crs.process_id as processId",
      "cr.cluster_id as clusterId",
      "crs.updated_at as lastOfflineAt",
    ])
    .select((eb) =>
      eb
        .case()
        .when("crs.reason", "=", sql.lit("oom-killed"))
        .then(sql.raw("true"))
        .else(sql.raw("false"))
        .end()
        .$castTo<boolean>()
        .as("isOom"),
    )
    .where((eb) =>
      eb("status", "in", [sql.lit("not-ready"), sql.lit("offline")]),
    );
}

export type LatestOfflineReplica = InferResult<
  ReturnType<typeof buildLatestOfflineReplicaQuery>
>[0];

function buildLatestOfflineReplicaSubscribe<T>(minDate: Date) {
  return sql<T>`SUBSCRIBE (${buildLatestOfflineReplicaQuery()}) WITH(PROGRESS) AS OF AT LEAST TIMESTAMP ${sql.lit(
    minDate.toISOString(),
  )} ENVELOPE UPSERT (KEY ("replicaId", "processId"));`;
}

// If an OOM happened within OOM_SURFACE_GRACE_PERIOD_MINUTES ago, we alert it. This number is arbitrary.
const OOM_SURFACE_GRACE_PERIOD_MINUTES = 15;

export const shouldSurfaceOom = (currentTime: Date, lastOomAt: Date) => {
  const gracePeriodDate = subMinutes(
    currentTime,
    OOM_SURFACE_GRACE_PERIOD_MINUTES,
  );
  return isAfter(lastOomAt, gracePeriodDate);
};

/**
 * Initiates a websocket connection and returns a map of a cluster id to the most recent offline status in any of its replicas
 */
const useLatestOfflineReplica = () => {
  const subscribe = React.useMemo(() => {
    const now = new Date();
    const minFrontier = subMinutes(now, OOM_SURFACE_GRACE_PERIOD_MINUTES);
    return buildLatestOfflineReplicaSubscribe<LatestOfflineReplica>(
      minFrontier,
    );
  }, []);

  const { data, ...rest } = useSubscribe({
    subscribe,
    closeSocketOnComplete: true,
    upsertKey: (row) => row.data.replicaId + "-" + row.data.processId,
  });

  const result = React.useMemo(() => {
    const map = new Map();
    for (const row of data) {
      const { lastOfflineAt: lastOfflineAt, isOom, clusterId } = row.data;
      if (!lastOfflineAt) continue;

      const newLatestOfflineReplicaInfo: LatestOfflineReplicaInfo = {
        lastOfflineAt: lastOfflineAt,
        shouldSurfaceOom: isOom && shouldSurfaceOom(new Date(), lastOfflineAt),
      };
      map.set(clusterId, newLatestOfflineReplicaInfo);
    }

    return map;
  }, [data]);

  return {
    data: result,
    ...rest,
  };
};

export type LatestOfflineReplicaResult = ReturnType<
  typeof useLatestOfflineReplica
>;

export default useLatestOfflineReplica;
