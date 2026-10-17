// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom, useAtomValue } from "jotai";
import React from "react";

import {
  buildReplicaProcessMetricsSubscribe,
  ProcessMetricsSample,
  processSampleKey,
} from "~/api/materialize/cluster/replicaProcessMetrics";
import {
  SubscribeRow,
  SubscribeState,
} from "~/api/materialize/SubscribeManager";
import { useGlobalUpsertSubscribe } from "~/api/materialize/useSubscribe";
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
  const stateAtom = React.useMemo(
    () =>
      atom<SubscribeState<ProcessMetricsSample>>({
        data: [],
        snapshotComplete: false,
        error: undefined,
      }),
    [],
  );
  const options = React.useMemo(
    () => ({
      atom: stateAtom,
      subscribe:
        startMs === undefined
          ? undefined
          : buildReplicaProcessMetricsSubscribe({
              replicaIds: [replicaId],
              since: new Date(startMs),
              includeSwap: false,
            }),
      select: (row: SubscribeRow<ProcessMetricsSample>) => row.data,
      upsertKey: (row: SubscribeRow<ProcessMetricsSample>) =>
        processSampleKey(row.data),
    }),
    [stateAtom, startMs, replicaId],
  );
  useGlobalUpsertSubscribe(options);
  return useAtomValue(stateAtom);
}
