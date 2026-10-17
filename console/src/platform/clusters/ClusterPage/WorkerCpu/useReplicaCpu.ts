// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useLiveQuery } from "@tanstack/react-db";
import { subMinutes } from "date-fns";
import { useAtomValue } from "jotai";
import React from "react";

import {
  buildReplicaProcessMetricsSubscribe,
  ProcessMetricsSample,
  processSampleKey,
} from "~/api/materialize/cluster/replicaProcessMetrics";
import { createScopedSubscribeCollections } from "~/api/materialize/subscribeCollection";
import { SubscribeRow } from "~/api/materialize/SubscribeManager";
import { useGlobalSubscribeCollection } from "~/api/materialize/useSubscribe";
import { syncEngineCacheScopeLoadableAtom } from "~/store/syncEngineCache";

import { replicaCpuPoints } from "./workerCpuModel";

// Covers the drawer's chart window and the lead before its first reading.
const RETENTION_MINUTES = 20;

const metricsByReplica = createScopedSubscribeCollections<ProcessMetricsSample>(
  { id: "worker-cpu-replica-metrics", getKey: processSampleKey },
);

/**
 * A replica's CPU since `RETENTION_MINUTES` before the calling component
 * mounted, from its raw per-minute metrics, streamed while it stays mounted. The resource
 * usage card's series can't serve the drawer: past a 1h window they are
 * per-bucket peaks, and past 24h they stop at the last poll.
 */
export function useReplicaCpu({
  replicaId,
  cpuNanoCoresPerProcess,
}: {
  replicaId: string;
  cpuNanoCoresPerProcess: number;
}) {
  const scope = useAtomValue(syncEngineCacheScopeLoadableAtom);
  const samples =
    scope.state === "hasData" && scope.data
      ? metricsByReplica.get(scope.data, replicaId)
      : undefined;
  const options = React.useMemo(
    () => ({
      target: samples ?? metricsByReplica.placeholder,
      subscribe: samples
        ? buildReplicaProcessMetricsSubscribe({
            replicaIds: [replicaId],
            since: subMinutes(new Date(), RETENTION_MINUTES),
            includeSwap: false,
          })
        : undefined,
      select: (row: SubscribeRow<ProcessMetricsSample>) => row.data,
      upsertKey: (row: SubscribeRow<ProcessMetricsSample>) =>
        processSampleKey(row.data),
    }),
    [samples, replicaId],
  );
  useGlobalSubscribeCollection(options);
  const { data: rows } = useLiveQuery(
    (q) => (samples ? q.from({ sample: samples.collection }) : undefined),
    [samples],
  );
  return React.useMemo(
    () => replicaCpuPoints(rows ?? [], cpuNanoCoresPerProcess),
    [rows, cpuNanoCoresPerProcess],
  );
}
