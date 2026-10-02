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
  buildReplicaProcessMetricsSubscribe,
  processSampleKey,
} from "~/api/materialize/cluster/replicaProcessMetrics";
import { useSubscribe } from "~/api/materialize/useSubscribe";
import { useRollingWindowStartMs } from "~/platform/clusters/queries";

/**
 * A replica's per-process samples over at least the last `windowMinutes`,
 * streamed while the calling component is mounted.
 */
export function useReplicaProcessSamples({
  replicaId,
  windowMinutes,
}: {
  replicaId: string;
  windowMinutes: number;
}) {
  const startMs = useRollingWindowStartMs(windowMinutes);
  const subscribe = React.useMemo(
    () =>
      startMs === undefined
        ? undefined
        : buildReplicaProcessMetricsSubscribe({
            replicaIds: [replicaId],
            since: new Date(startMs),
          }),
    [replicaId, startMs],
  );
  const { data, snapshotComplete, error } = useSubscribe({
    subscribe,
    upsertKey: (row) => processSampleKey(row.data),
    select: (row) => row.data,
  });
  return { data, snapshotComplete, error };
}
