// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useSearchParams } from "react-router-dom";

import { Cluster } from "~/api/materialize/cluster/clusterList";

export const REPLICA_SEARCH_PARAM = "replica";

/**
 * The replica the Metrics page is narrowed to, by name in the `replica` search
 * param, so a narrowed view is a link. Undefined means all replicas, which is
 * also what a name no current replica has reads as. The stale name stays in
 * the URL, so the selection comes back if a replica of that name does.
 */
export function useSelectedReplica(cluster: Cluster) {
  const [searchParams, setSearchParams] = useSearchParams();
  const name = searchParams.get(REPLICA_SEARCH_PARAM);
  const selectedReplica =
    name === null
      ? undefined
      : cluster.replicas.find((replica) => replica.name === name);

  /** Undefined selects all replicas. */
  const setSelectedReplicaName = (replicaName: string | undefined) =>
    setSearchParams(
      (prev) => {
        if (replicaName === undefined) prev.delete(REPLICA_SEARCH_PARAM);
        else prev.set(REPLICA_SEARCH_PARAM, replicaName);
        return prev;
      },
      // Switching replicas would otherwise fill the back button's history.
      { replace: true },
    );

  return { selectedReplica, setSelectedReplicaName };
}
