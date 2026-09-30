// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { VStack } from "@chakra-ui/react";
import React from "react";
import { Navigate, Route, useParams } from "react-router-dom";

import { useUiPreview } from "~/hooks/useUiPreview";
import {
  Breadcrumb,
  PageHeader,
  PageTabStrip,
  Tab,
} from "~/layouts/BaseLayout";
import { ClusterDetailParams } from "~/platform/clusters/ClusterRoutes";
import { SentryRoutes } from "~/sentry";

import { ClusterDetailBreadcrumbs } from "./ClusterDetailBreadcrumbs";
import ClusterOverview from "./ClusterOverview";
import { CLUSTER_OBJECT_TYPES } from "./ClusterPage/clusterObjectTypes";
import ClusterPage from "./ClusterPage/ClusterPage";
import ClusterReplicas from "./ClusterReplicas";
import IndexList from "./IndexList";
import MaterializedViewsList from "./MaterializedViewsList";
import Sinks from "./Sinks";
import Sources from "./Sources";

// The redesigned page nests the object lists under `objects/`. Send those links
// to the matching classic tab so they still land somewhere for opted-out users.
// A bare `objects` link opens the first type, as the redesigned page does.
const ClassicObjectsRedirect = () => {
  const { objectType = CLUSTER_OBJECT_TYPES[0].path } = useParams<{
    objectType: string;
  }>();
  const isKnownType = CLUSTER_OBJECT_TYPES.some(
    ({ path }) => path === objectType,
  );
  return <Navigate to={isKnownType ? `../${objectType}` : ".."} replace />;
};

const ClassicClusterDetailPage = () => {
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
        <Route path="objects" element={<ClassicObjectsRedirect />} />
        <Route
          path="objects/:objectType"
          element={<ClassicObjectsRedirect />}
        />
      </SentryRoutes>
    </>
  );
};

const ClusterDetailPage = () => {
  const { isEnabled } = useUiPreview("clusterDetailsRedesign");
  return isEnabled ? <ClusterPage /> : <ClassicClusterDetailPage />;
};

export default ClusterDetailPage;
