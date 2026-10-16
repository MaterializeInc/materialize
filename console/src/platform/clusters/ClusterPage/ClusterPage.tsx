// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Tag } from "@chakra-ui/react";
import React from "react";
import { Navigate, Route, useParams, useSearchParams } from "react-router-dom";

import { PageHeading, PageTabStrip, Tab } from "~/layouts/BaseLayout";
import ClusterReplicas from "~/platform/clusters/ClusterReplicas";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import { SentryRoutes } from "~/sentry";
import { useAllClusters } from "~/store/allClusters";
import { useSubscribeToAllObjectsCollection } from "~/store/allObjectsCollection";
import { assert, pluralize } from "~/util";

import { ClusterMetrics } from "./ClusterMetrics";
import { ClusterObjects } from "./ClusterObjects";
import { CLUSTER_OBJECT_TYPES } from "./clusterObjectTypes";
import { ClusterWorkers } from "./ClusterWorkers";
import { replicaSearch, WORKERS_TAB_PATH } from "./tabLinks";
import { REPLICA_SEARCH_PARAM } from "./useSelectedReplica";

// The Metrics and Workers tabs share a replica choice, so their links carry it.
const tabs = (replicaName: string | undefined): Tab[] => [
  { label: "Metrics", href: `..${replicaSearch(replicaName)}`, end: true },
  {
    label: "Workers",
    href: `../${WORKERS_TAB_PATH}${replicaSearch(replicaName)}`,
  },
  { label: "Objects", href: "../objects" },
  // TODO: drop this tab once the cluster configuration drawer manages replicas.
  { label: "Replicas", href: "../replicas" },
];

const replicaCountLabel = (count: number) =>
  count === 0
    ? "No replicas"
    : `${count} ${pluralize(count, "replica", "replicas")}`;

// The all-clusters subscribe updates on any cluster's replica status change.
// Looking the cluster up here keeps those updates from re-rendering the page.
const ClusterTitle = ({
  name,
  clusterId,
}: {
  name: string;
  clusterId: string;
}) => {
  const { getClusterById } = useAllClusters();
  const cluster = getClusterById(clusterId);
  return (
    <HStack px={7} pt={4} pb={1} spacing={3}>
      <PageHeading as="h1">{name}</PageHeading>
      {cluster && (
        <HStack spacing={2}>
          {cluster.managed && cluster.size && (
            <Tag size="sm">{cluster.size}</Tag>
          )}
          <Tag size="sm">{replicaCountLabel(cluster.replicas.length)}</Tag>
          {!cluster.managed && <Tag size="sm">Unmanaged</Tag>}
        </HStack>
      )}
    </HStack>
  );
};

/** The redesigned page's part of the shared header: its title row and tabs. */
export const ClusterPageHeader = () => {
  const { clusterId, clusterName } = useParams<ClusterParams>();
  const [searchParams] = useSearchParams();
  assert(clusterId);
  assert(clusterName);
  return (
    <>
      <ClusterTitle name={clusterName} clusterId={clusterId} />
      <PageTabStrip
        tabData={tabs(searchParams.get(REPLICA_SEARCH_PARAM) ?? undefined)}
      />
    </>
  );
};

const ClusterPage = () => {
  const { clusterName } = useParams<ClusterParams>();
  assert(clusterName);
  useSubscribeToAllObjectsCollection();

  // Keying route elements on the cluster name avoids stale state when the
  // breadcrumb menu switches clusters.
  return (
    <SentryRoutes>
      <Route index element={<ClusterMetrics key={clusterName} />} />
      <Route
        path={WORKERS_TAB_PATH}
        element={<ClusterWorkers key={clusterName} />}
      />
      <Route
        path="objects"
        element={<Navigate to={CLUSTER_OBJECT_TYPES[0].path} replace />}
      />
      <Route
        path="objects/:objectType"
        element={<ClusterObjects key={clusterName} />}
      />
      <Route path="replicas" element={<ClusterReplicas key={clusterName} />} />
      {/* The classic page's tab URLs, kept working for bookmarks and shared links. */}
      {CLUSTER_OBJECT_TYPES.map(({ path }) => (
        <Route
          key={path}
          path={path}
          element={<Navigate to={`../objects/${path}`} replace />}
        />
      ))}
    </SentryRoutes>
  );
};

export default ClusterPage;
