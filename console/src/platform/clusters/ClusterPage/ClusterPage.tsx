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
import { Navigate, Route, useParams } from "react-router-dom";

import { PageHeading, PageTabStrip, Tab } from "~/layouts/BaseLayout";
import ClusterOverview from "~/platform/clusters/ClusterOverview";
import ClusterReplicas from "~/platform/clusters/ClusterReplicas";
import { ClusterParams } from "~/platform/clusters/ClusterRoutes";
import { SentryRoutes } from "~/sentry";
import { useAllClusters } from "~/store/allClusters";
import { assert, pluralize } from "~/util";

import { ClusterObjects } from "./ClusterObjects";
import { CLUSTER_OBJECT_TYPES } from "./clusterObjectTypes";

const TABS: Tab[] = [
  { label: "Metrics", href: "..", end: true },
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
  assert(clusterId);
  assert(clusterName);
  return (
    <>
      <ClusterTitle name={clusterName} clusterId={clusterId} />
      <PageTabStrip tabData={TABS} />
    </>
  );
};

const ClusterPage = () => {
  const { clusterName } = useParams<ClusterParams>();
  assert(clusterName);

  // Keying route elements on the cluster name avoids stale state when the
  // breadcrumb menu switches clusters.
  return (
    <SentryRoutes>
      <Route index element={<ClusterOverview key={clusterName} />} />
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
