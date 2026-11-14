// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Flex } from "@chakra-ui/react";
import React from "react";
import { Navigate, Route } from "react-router-dom";

import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import {
  PageBreadcrumbs,
  PageHeader,
  PageTabStrip,
  Tab,
} from "~/layouts/BaseLayout";
import { regionPath } from "~/platform/routeHelpers";
import { SentryRoutes } from "~/sentry";
import { useSubscribeToAllObjectsCollection } from "~/store/allObjectsCollection";
import { useRegionSlug } from "~/store/environments";
import { useSubscribeToWorkflowGraphEdgesCollection } from "~/store/workflowGraphEdgesCollection";

import { CatalogLens } from "./CatalogLens";
import { DataProductsLens } from "./DataProductsLens";

const OntologyRoutes = () => {
  const regionSlug = useRegionSlug();
  // Relative links inside a splat route resolve against the matched URL, so a
  // relative "catalog" from /ontology/catalog would nest, hence absolute.
  const base = `${regionPath(regionSlug)}/ontology`;
  const tabs: Tab[] = React.useMemo(
    () => [
      { label: "Data products", href: base, end: true },
      { label: "System catalog", href: `${base}/catalog` },
    ],
    [base],
  );
  // The data products lens reads objects and lineage from these collections.
  useSubscribeToAllObjectsCollection();
  useSubscribeToWorkflowGraphEdgesCollection();
  return (
    <Flex direction="column" height="100%" minHeight={0}>
      <PageHeader variant="compact" boxProps={{ mb: 0 }}>
        <PageBreadcrumbs crumbs={[{ title: "Ontology" }]} />
        <PageTabStrip tabData={tabs} />
      </PageHeader>
      <AppErrorBoundary message="An error occurred loading the ontology.">
        <SentryRoutes>
          <Route index element={<DataProductsLens />} />
          <Route path="catalog" element={<CatalogLens />} />
          <Route path="*" element={<Navigate to={base} replace />} />
        </SentryRoutes>
      </AppErrorBoundary>
    </Flex>
  );
};

export default OntologyRoutes;
