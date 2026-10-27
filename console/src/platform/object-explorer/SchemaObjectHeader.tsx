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

import { DeletableObjectType } from "~/api/materialize/buildDropObjectStatement";
import { DatabaseObject } from "~/api/materialize/types";
import DeleteObjectMenuItem from "~/components/DeleteObjectMenuItem";
import OverflowMenu from "~/components/OverflowMenu";
import {
  Breadcrumb,
  PageBreadcrumbs,
  PageHeader,
  PageTabStrip,
  Tab,
} from "~/layouts/BaseLayout";
import { useRegionSlug } from "~/store/environments";
import { kebabToScreamingSpaceCase } from "~/util";

import { databasePath } from "../routeHelpers";
import { useIsOwner } from "./queries";
import { useSchemaObjectParams } from "./useSchemaObjectParams";

export const SchemaObjectHeader = ({
  tabStripItems,
  objectType,
}: {
  tabStripItems: Tab[];
  objectType?: string;
}) => {
  const params = useSchemaObjectParams();
  const regionSlug = useRegionSlug();

  const object = {
    id: params.id,
    schemaName: params.schemaName,
    databaseName: params.databaseName,
    name: params.objectName,
  };

  const breadcrumbs: Breadcrumb[] = [
    {
      title: params.databaseName,
      href: databasePath(regionSlug, params.databaseName),
    },
    { title: params.schemaName, href: "../.." },
    { title: params.objectName, href: ".." },
  ];

  return (
    <PageHeader variant="compact" boxProps={{ mb: 0 }} sticky>
      <VStack spacing={0} alignItems="flex-start" width="100%">
        <HStack width="100%" justifyContent="space-between">
          <PageBreadcrumbs
            crumbs={breadcrumbs}
            rightSideChildren={
              objectType && (
                <React.Suspense>
                  <OverflowMenuContainer
                    object={object}
                    objectType={objectType}
                  />
                </React.Suspense>
              )
            }
          />
        </HStack>
        <PageTabStrip tabData={tabStripItems} />
      </VStack>
    </PageHeader>
  );
};

const OverflowMenuContainer = (props: {
  object: DatabaseObject;
  objectType: string;
}) => {
  const { data: isOwner } = useIsOwner({ objectId: props.object.id });
  return (
    <OverflowMenu
      items={[
        {
          visible: isOwner,
          render: () => (
            <DeleteObjectMenuItem
              key="delete-object"
              selectedObject={props.object}
              onSuccessAction={() => undefined}
              objectType={
                kebabToScreamingSpaceCase(
                  props.objectType,
                ) as DeletableObjectType
              }
            />
          ),
        },
      ]}
    />
  );
};
