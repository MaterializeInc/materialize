// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

'use strict';

// NOTE: this file and the page script that consumes it are separate classic
// scripts sharing one global scope, so a top-level `const { useState } = React`
// here would collide with the same declaration in the page script. Reach
// through `React` instead.

/**
 * The cluster replica picker shared by the memory visualizer pages.
 *
 * Renders `props.children(clusterName, replicaName)` once a replica is settled,
 * which is the one named in the URL if there is one and otherwise the one
 * chosen below. The choice is mirrored back into the URL, so a page can be
 * linked to a specific replica.
 */
function ClusterReplicaView(props) {
  const [currentClusterName, setCurrentClusterName] = React.useState(null);
  const [currentReplicaName, setCurrentReplicaName] = React.useState(null);
  const [replicas, setReplicas] = React.useState(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState(false);

  // The first statement names the session's cluster, which is the starting
  // point for the choice below. `mz_clusters.id` starts with `u` for the
  // clusters a user created, which is what separates them from the builtins.
  const queryClusterReplicas = `
    SELECT current_setting('cluster');

    SELECT
      clusters.name AS cluster_name,
      replicas.name AS replica_name,
      clusters.id LIKE 'u%' AS user_cluster
    FROM
      mz_catalog.mz_cluster_replicas replicas
      LEFT JOIN mz_catalog.mz_clusters clusters ON clusters.id = replicas.cluster_id
    ORDER BY cluster_name ASC, replica_name ASC
  `;

  React.useEffect(() => {
    const search = new URLSearchParams(location.search);
    const clusterName = search.get('cluster_name');
    const replicaName = search.get('replica_name');
    if (clusterName) {
      setCurrentClusterName(clusterName);
    }
    if (replicaName) {
      setCurrentReplicaName(replicaName);
    }

    query(queryClusterReplicas)
      .then((data) => {
        const [sessionClusterTable, replicasTable] = data.results;
        const sessionCluster = sessionClusterTable.rows[0][0];
        const replicas = replicasTable.rows.map(
          ([clusterName, replicaName, userCluster]) => ({
            clusterName,
            replicaName,
            userCluster,
          })
        );
        setReplicas(replicas);
        if (!replicaName && replicas.length > 0) {
          // A user cluster is preferred over the session's own, because this
          // page is normally reached through a proxy that authenticates as
          // `mz_support`, whose default cluster is the builtin
          // `mz_catalog_server`. Nobody opens the dataflow visualizer to look
          // at the catalog server, and its introspection relations are an
          // order of magnitude more expensive to query than a small user
          // cluster's. Rows are ordered by cluster name and then replica name,
          // so each fallback lands on the first replica of the first cluster
          // that qualifies.
          const preferred =
            replicas.find(
              (r) => r.userCluster && r.clusterName === sessionCluster
            ) ||
            replicas.find((r) => r.userCluster) ||
            replicas[0];
          setCurrentClusterName(preferred.clusterName);
          setCurrentReplicaName(preferred.replicaName);
        }
        setLoading(false);
      })
      .catch((error) => {
        setError(error);
        setLoading(false);
      });
  }, []);

  React.useEffect(() => {
    if (!currentReplicaName) return;
    const params = new URLSearchParams(location.search);
    params.set('cluster_name', currentClusterName);
    params.set('replica_name', currentReplicaName);
    window.history.replaceState({}, '', `${location.pathname}?${params}`);
  }, [currentClusterName, currentReplicaName]);

  return (
    <div>
      {loading ? (
        <div>Loading...</div>
      ) : error ? (
        <div>error: {String(error)}</div>
      ) : (
        <div>
          <label htmlFor="cluster_replica">Cluster Replica </label>
          <select
            id="cluster_replica"
            name="cluster_replica"
            onChange={(event) => {
              const [clusterName, replicaName] = JSON.parse(event.target.value);
              setCurrentClusterName(clusterName);
              setCurrentReplicaName(replicaName);
            }}
            defaultValue={JSON.stringify([currentClusterName, currentReplicaName])}
          >
            {replicas.map(({ clusterName, replicaName }) => {
              const value = JSON.stringify([clusterName, replicaName]);
              return (
                <option key={value} value={value}>
                  {`${clusterName}.${replicaName}`}
                </option>
              );
            })}
          </select>
          {props.children(currentClusterName, currentReplicaName)}
        </div>
      )}
    </div>
  );
}
