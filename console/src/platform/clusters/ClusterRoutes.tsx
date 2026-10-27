// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Navigate, Route, useParams } from "react-router-dom";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import ClusterDetailPage from "~/platform/clusters/ClusterDetail";
import ClustersListPage from "~/platform/clusters/ClustersList";
import { relativeClusterPath } from "~/platform/routeHelpers";
import { SentryRoutes } from "~/sentry";
import { useAllClusters } from "~/store/allClusters";

import { CLUSTERS_FETCH_ERROR_MESSAGE } from "./constants";
import NewClusterForm from "./NewClusterForm";

export type ClusterDetailParams = {
  regionSlug: string;
  clusterName: string;
};

const ClusterRoutes = () => {
  return (
    <AppErrorBoundary message={CLUSTERS_FETCH_ERROR_MESSAGE}>
      <SentryRoutes>
        <Route index element={<ClustersListPage />} />
        <Route path="new" element={<NewClusterForm />} />
        <Route path=":clusterId/:clusterName">
          <Route index path="*" element={<ClusterOrRedirect />} />
        </Route>
      </SentryRoutes>
    </AppErrorBoundary>
  );
};

export type ClusterParams = {
  clusterId: string;
  clusterName: string;
};

const handleRenamedCluster = (
  cluster: Cluster,
  params: Readonly<Partial<ClusterParams>>,
) => {
  if (cluster.name !== params.clusterName) {
    return <Navigate to={`../../${relativeClusterPath(cluster)}`} replace />;
  }
  return <ClusterDetailPage />;
};

const ClusterOrRedirect: React.FC = () => {
  const params = useParams<ClusterParams>();
  const { snapshotComplete, data: clusters } = useAllClusters();
  // Show loading state until clusters load
  if (!snapshotComplete) {
    return <ClusterDetailPage />;
  }
  let cluster = clusters?.find((c) => c.id === params.clusterId);
  if (cluster) {
    return handleRenamedCluster(cluster, params);
  }
  cluster = clusters?.find((c) => c.name === params.clusterName);
  if (cluster) {
    // since the ID didn't match, update the url
    return <Navigate to={`../../${relativeClusterPath(cluster)}`} replace />;
  }
  // Cluster not found, redirect to cluster list
  return <Navigate to="../.." replace />;
};

export default ClusterRoutes;
