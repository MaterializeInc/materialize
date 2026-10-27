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
import { useParams } from "react-router-dom";

import { DatabaseDetails as DatabaseDetailsType } from "~/api/materialize/object-explorer/databaseDetails";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import {
  Breadcrumb,
  MainContentContainer,
  PageBreadcrumbs,
  PageHeader,
  PageTabStrip,
  Tab,
} from "~/layouts/BaseLayout";
import { assert } from "~/util";

import { ObjectDetailsContainer, ObjectDetailsStrip } from "./detailComponents";
import { useDatabaseDetails } from "./queries";

export const DatabaseDetails = () => {
  const { databaseName } = useParams<{ databaseName: string }>();
  assert(
    databaseName,
    "DatabaseDetails expects databaseName param to be defined.",
  );
  const tabStripItems: Tab[] = [
    {
      label: "Details",
      href: `.`,
      end: true,
    },
  ];
  const breadcrumbs: Breadcrumb[] = [{ title: databaseName, href: "." }];

  return (
    <>
      <PageHeader variant="compact" boxProps={{ mb: 0 }} sticky>
        <VStack spacing={0} alignItems="flex-start" width="100%">
          <PageBreadcrumbs crumbs={breadcrumbs} />
          <PageTabStrip tabData={tabStripItems} />
        </VStack>
      </PageHeader>
      <AppErrorBoundary message="An error occurred loading database details.">
        <React.Suspense fallback={<LoadingContainer />}>
          <DatabaseDetailsLoader name={databaseName} />
        </React.Suspense>
      </AppErrorBoundary>
    </>
  );
};

export interface DatabaseDetailsParams {
  name: string;
}

export const DatabaseDetailsLoader = (props: DatabaseDetailsParams) => {
  const { data: database } = useDatabaseDetails(props);

  return <DatabaseDetailsContent database={database} />;
};

export interface DatabaseDetailsContentParams {
  database: DatabaseDetailsType;
}

export const DatabaseDetailsContent = ({
  database,
}: DatabaseDetailsContentParams) => {
  return (
    <MainContentContainer>
      <ObjectDetailsContainer>
        <ObjectDetailsStrip type="database" {...database} />
      </ObjectDetailsContainer>
    </MainContentContainer>
  );
};
