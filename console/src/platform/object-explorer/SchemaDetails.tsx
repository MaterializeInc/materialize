// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HStack, VStack } from "@chakra-ui/react";
import React from "react";
import { useParams } from "react-router-dom";

import { SchemaDetails as SchemaDetailsType } from "~/api/materialize/object-explorer/schemaDetails";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import { LoadingContainer } from "~/components/LoadingContainer";
import OverflowMenu from "~/components/OverflowMenu";
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
import { useSchemaDetails } from "./queries";
import { relativeDatabasePath } from "./routerHelpers";

export const SchemaDetails = () => {
  const { schemaName, databaseName } = useParams<{
    schemaName: string;
    databaseName: string;
  }>();
  assert(schemaName, "SchemaDetails expects schemaName param to be defined.");
  assert(
    databaseName,
    "SchemaDetails expects databaseName param to be defined.",
  );
  const tabStripItems: Tab[] = [
    {
      label: "Details",
      href: `.`,
      end: true,
    },
  ];
  const breadcrumbs: Breadcrumb[] = [
    {
      title: databaseName,
      href: `../${relativeDatabasePath({ databaseName })}`,
    },
    { title: schemaName, href: "." },
  ];

  return (
    <>
      <PageHeader variant="compact" boxProps={{ mb: 0 }} sticky>
        <VStack spacing={0} alignItems="flex-start" width="100%">
          <HStack width="100%" justifyContent="space-between">
            <PageBreadcrumbs
              crumbs={breadcrumbs}
              rightSideChildren={
                <AppErrorBoundary renderFallback={() => null}>
                  <React.Suspense>
                    <OverflowMenuContainer
                      name={schemaName}
                      databaseName={databaseName}
                    />
                  </React.Suspense>
                </AppErrorBoundary>
              }
            />
          </HStack>
          <PageTabStrip tabData={tabStripItems} />
        </VStack>
      </PageHeader>
      <AppErrorBoundary message="An error occurred loading schema details.">
        <React.Suspense fallback={<LoadingContainer />}>
          <SchemaDetailsLoader databaseName={databaseName} name={schemaName} />
        </React.Suspense>
      </AppErrorBoundary>
    </>
  );
};

export interface SchemaDetailsParams {
  databaseName: string;
  name: string;
}

export const SchemaDetailsLoader = (props: SchemaDetailsParams) => {
  const { data: schema } = useSchemaDetails(props);

  return <SchemaDetailsContent schema={schema} />;
};

export interface SchemaDetailsContentParams {
  schema: SchemaDetailsType;
}

export const SchemaDetailsContent = ({
  schema,
}: SchemaDetailsContentParams) => {
  return (
    <MainContentContainer>
      <ObjectDetailsContainer>
        <ObjectDetailsStrip type="schema" {...schema} />
      </ObjectDetailsContainer>
    </MainContentContainer>
  );
};

const OverflowMenuContainer = (props: {
  name: string;
  databaseName: string;
}) => {
  const { data: schema } = useSchemaDetails(props);
  return (
    <OverflowMenu
      items={[
        {
          visible: schema.isOwner,
          render: () => (
            <DeleteObjectMenuItem
              key="delete-object"
              selectedObject={schema}
              onSuccessAction={() => undefined}
              objectType="SCHEMA"
            />
          ),
        },
      ]}
    />
  );
};
