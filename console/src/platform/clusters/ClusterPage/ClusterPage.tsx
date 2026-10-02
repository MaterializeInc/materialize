// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, Tag, VStack } from "@chakra-ui/react";
import React from "react";
import { Navigate, Route, useParams } from "react-router-dom";

import { Cluster } from "~/api/materialize/cluster/clusterList";
import {
  Breadcrumb,
  PageHeader,
  PageHeading,
  PageTabStrip,
  Tab,
} from "~/layouts/BaseLayout";
import { ClusterDetailBreadcrumbs } from "~/platform/clusters/ClusterDetailBreadcrumbs";
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

const ClusterTitle = ({
  name,
  cluster,
}: {
  name: string;
  cluster: Cluster | undefined;
}) => (
  <HStack px={7} pt={4} pb={1} spacing={3}>
    <PageHeading as="h1">{name}</PageHeading>
    {cluster && (
      <HStack spacing={2}>
        {cluster.managed && cluster.size && <Tag size="sm">{cluster.size}</Tag>}
        <Tag size="sm">{replicaCountLabel(cluster.replicas.length)}</Tag>
        {!cluster.managed && <Tag size="sm">Unmanaged</Tag>}
      </HStack>
    )}
  </HStack>
);

const ClusterPage = () => {
  const { clusterId, clusterName } = useParams<ClusterParams>();
  assert(clusterId);
  assert(clusterName);
  const { getClusterById } = useAllClusters();
  const cluster = getClusterById(clusterId);

  const breadcrumbs: Breadcrumb[] = React.useMemo(
    () => [
      { title: "Clusters", href: "../.." },
      { title: clusterName, href: ".." },
    ],
    [clusterName],
  );

  // Keying route elements on the cluster name avoids stale state when the
  // breadcrumb menu switches clusters.
  return (
    <>
      <PageHeader variant="compact" boxProps={{ mb: 0 }} sticky>
        <VStack spacing={0} alignItems="flex-start" width="100%">
          <ClusterDetailBreadcrumbs crumbs={breadcrumbs} />
          <ClusterTitle name={clusterName} cluster={cluster} />
          <PageTabStrip tabData={TABS} />
        </VStack>
      </PageHeader>
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
        <Route
          path="replicas"
          element={<ClusterReplicas key={clusterName} />}
        />
        {/* The classic page's tab URLs, kept working for bookmarks and shared links. */}
        {CLUSTER_OBJECT_TYPES.map(({ path }) => (
          <Route
            key={path}
            path={path}
            element={<Navigate to={`../objects/${path}`} replace />}
          />
        ))}
      </SentryRoutes>
    </>
  );
};

export default ClusterPage;
