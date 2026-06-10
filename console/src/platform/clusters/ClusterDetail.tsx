// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { MenuItem, VStack } from "@chakra-ui/react";
import React from "react";
import { Link, Route, useLocation, useParams } from "react-router-dom";

import { isSystemCluster } from "~/api/materialize";
import { ClusterWithOwnership } from "~/api/materialize/cluster/clusterList";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import OverflowMenu from "~/components/OverflowMenu";
import {
  Breadcrumb,
  PageBreadcrumbs,
  PageHeader,
  PageTabStrip,
  Tab,
} from "~/layouts/BaseLayout";
import {
  ClusterDetailParams,
  ClusterParams,
} from "~/platform/clusters/ClusterRoutes";
import { SentryRoutes } from "~/sentry";
import { assert } from "~/util";

import { replaceClusterIdAndName } from "../routeHelpers";
import AlterClusterMenuItem from "./AlterClusterMenuItem";
import ClusterOverview from "./ClusterOverview";
import ClusterReplicas from "./ClusterReplicas";
import IndexList from "./IndexList";
import MaterializedViewsList from "./MaterializedViewsList";
import { useClusters } from "./queries";
import Sinks from "./Sinks";
import Sources from "./Sources";
import { useShowSystemObjects } from "./useShowSystemObjects";

const ClusterDetailBreadcrumbs = (props: { crumbs: Breadcrumb[] }) => {
  const [showSystemObjects] = useShowSystemObjects();
  const { clusterId, clusterName } = useParams<ClusterParams>();
  const { data: clusters, getClusterById } = useClusters();
  const { pathname, search } = useLocation();
  assert(clusterId);
  assert(clusterName);
  const cluster = getClusterById(clusterId);

  const clustersToShow = showSystemObjects
    ? clusters
    : clusters.filter((c) => c.id.startsWith("u"));

  const menu = (
    <>
      {clustersToShow.map((c) => (
        <MenuItem
          as={Link}
          disabled={c.name === clusterName}
          to={
            replaceClusterIdAndName({
              pathname,
              currentClusterId: clusterId,
              currentClusterName: clusterName,
              targetCluster: c,
            }) + search
          }
          key={c.id}
        >
          {c.name}
        </MenuItem>
      ))}
    </>
  );

  return (
    <PageBreadcrumbs
      crumbs={props.crumbs}
      contextMenuChildren={menu}
      rightSideChildren={cluster && <OverflowMenuContainer cluster={cluster} />}
    />
  );
};

const OverflowMenuContainer = ({
  cluster,
}: {
  cluster: ClusterWithOwnership;
}) => {
  return (
    <OverflowMenu
      items={[
        {
          visible: !isSystemCluster(cluster.id) && cluster.managed,
          render: () => <AlterClusterMenuItem cluster={cluster} />,
        },
        {
          visible: !isSystemCluster(cluster.id) && cluster?.isOwner,
          render: () =>
            cluster && (
              <DeleteObjectMenuItem
                key="delete-object"
                selectedObject={cluster}
                // subscribe will update our list and the cluster routes will redirect
                onSuccessAction={() => undefined}
                objectType="CLUSTER"
              />
            ),
        },
      ]}
    />
  );
};

const ClusterDetailPage = () => {
  const { clusterName } = useParams<ClusterDetailParams>();

  const breadcrumbs: Breadcrumb[] = React.useMemo(
    () => [
      { title: "Clusters", href: "../.." },
      { title: clusterName ?? "", href: ".." },
    ],
    [clusterName],
  );
  const subnavItems: Tab[] = React.useMemo(
    () => [
      { label: "Overview", href: "..", end: true },
      { label: "Replicas", href: "../replicas" },
      { label: "Materialized Views", href: "../materialized-views" },
      { label: "Indexes", href: "../indexes" },
      { label: "Sources", href: "../sources" },
      { label: "Sinks", href: "../sinks" },
    ],
    [],
  );

  // Setting key on the route elements prevents any weird jank when you use the context
  // menu to switch between clusters.
  return (
    <>
      <PageHeader variant="compact" boxProps={{ mb: 0 }} sticky>
        <VStack spacing={0} alignItems="flex-start" width="100%">
          <ClusterDetailBreadcrumbs crumbs={breadcrumbs} />
          <PageTabStrip tabData={subnavItems} />
        </VStack>
      </PageHeader>
      <SentryRoutes>
        <Route index path="/" element={<ClusterOverview key={clusterName} />} />
        <Route
          path="replicas"
          element={<ClusterReplicas key={clusterName} />}
        />
        <Route
          path="materialized-views"
          element={<MaterializedViewsList key={clusterName} />}
        />
        <Route path="indexes" element={<IndexList key={clusterName} />} />
        <Route path="sources" element={<Sources key={clusterName} />} />
        <Route path="sinks" element={<Sinks key={clusterName} />} />
      </SentryRoutes>
    </>
  );
};

export default ClusterDetailPage;
