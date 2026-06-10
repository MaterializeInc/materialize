// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Navigate, Outlet, Route } from "react-router-dom";

import { DatabaseObjectDetails } from "~/api/materialize/object-explorer/objectDetails";
import {
  SHOW_CREATE_OBJECT_TYPES,
  ShowCreateObjectType,
} from "~/api/materialize/showCreate";
import { ALLOWED_OBJECT_TYPES } from "~/api/materialize/workflowGraph";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import { ShowCreateBlock } from "~/components/ShowCreateBlock";
import WorkflowGraph from "~/components/WorkflowGraph/WorkflowGraph";
import { useFlags } from "~/hooks/useFlags";
import { MainContentContainer, Tab } from "~/layouts/BaseLayout";
import { SentryRoutes } from "~/sentry";
import { useAllObjects } from "~/store/allObjects";

import { ObjectDetailsContainer, ObjectDetailsStrip } from "./detailComponents";
import { ObjectColumns } from "./ObjectColumns";
import { ObjectIndexes } from "./ObjectIndexes";
import { useObjectDetails } from "./queries";
import { SchemaObjectHeader } from "./SchemaObjectHeader";
import { SourceOverviewContainer } from "./SourceDetailRoutes";
import { useSchemaObjectParams } from "./useSchemaObjectParams";
import { useToastIfObjectNotExtant } from "./useToastIfObjectNotExtant";

const DataflowVisualizer = React.lazy(
  () => import("~/platform/clusters/DataflowVisualizer"),
);

const SIMPLE_OBJECTS_WITH_INDEXES = ["materialized-view", "view", "table"];
const DATAFLOW_VISUALIZER_OBJECTS = ["materialized-view", "index"];

export const SimpleObjectDetailsContainer = () => {
  return (
    <AppErrorBoundary message="An error occurred loading object details.">
      <React.Suspense fallback={<LoadingContainer />}>
        <SimpleObjectDetailsLoader />
      </React.Suspense>
    </AppErrorBoundary>
  );
};

export const SimpleObjectDetailsLoader = () => {
  const {
    databaseName,
    schemaName,
    objectName: name,
  } = useSchemaObjectParams();

  const { data: object } = useObjectDetails({
    databaseName,
    schemaName,
    name,
  });

  return <SimpleObjectDetailsContent databaseObject={object} />;
};

export const SimpleObjectDetailsContent = ({
  databaseObject,
}: {
  databaseObject: DatabaseObjectDetails;
}) => {
  const { data: objects } = useAllObjects();
  const object = objects.find((o) => o.id === databaseObject.id);
  const canShowCreate = SHOW_CREATE_OBJECT_TYPES.includes(
    databaseObject.type as ShowCreateObjectType,
  );

  return (
    <MainContentContainer>
      <ObjectDetailsContainer>
        <ObjectDetailsStrip
          {...databaseObject}
          sourceType={object?.sourceType}
        />
        {canShowCreate && (
          <ShowCreateBlock
            {...databaseObject}
            objectType={databaseObject.type as ShowCreateObjectType}
          />
        )}
      </ObjectDetailsContainer>
    </MainContentContainer>
  );
};

export const SimpleObjectDetailRoutes = () => {
  const flags = useFlags();
  const { id } = useSchemaObjectParams();
  useToastIfObjectNotExtant();
  const { data: objects } = useAllObjects();
  const object = objects.find((o) => o.id === id);

  const dataflowVisualizerEnabled = flags["visualization-features"];

  // TODO: Remove this once we have webhook sources are tables (#3400)
  const isWebhookTable = !!object?.isWebhookTable;

  const tabStripItems: Tab[] = isWebhookTable
    ? [
        { label: "Overview", href: "..", end: true },
        { label: "Details", href: "../details", end: true },
      ]
    : [
        {
          label: "Details",
          href: "..",
          end: true,
        },
      ];

  const shouldShowWorkflowGraph =
    object &&
    // System objects don't have a database name and aren't supported by the workflow graph
    object.databaseId &&
    (ALLOWED_OBJECT_TYPES as string[]).includes(object.objectType);

  const shouldShowColumns =
    object && object.objectType !== "index" && object.objectType !== "secret";

  const shouldShowIndexes =
    object && SIMPLE_OBJECTS_WITH_INDEXES.includes(object.objectType);

  const shouldShowDataflowVisualizer =
    dataflowVisualizerEnabled &&
    object &&
    DATAFLOW_VISUALIZER_OBJECTS.includes(object.objectType);

  if (shouldShowWorkflowGraph) {
    tabStripItems.push({
      label: "Workflow",
      href: `../workflow`,
    });
  }
  if (shouldShowColumns) {
    tabStripItems.push({
      label: "Columns",
      href: `../columns`,
    });
  }

  if (shouldShowIndexes) {
    tabStripItems.push({
      label: "Indexes",
      href: `../indexes`,
    });
  }
  if (shouldShowDataflowVisualizer) {
    tabStripItems.push({
      label: "Visualize",
      href: `../dataflow-visualizer`,
    });
  }

  return (
    <SentryRoutes>
      <Route
        element={
          <>
            <SchemaObjectHeader
              tabStripItems={tabStripItems}
              objectType={object?.objectType}
            />

            <Outlet />
          </>
        }
      >
        {isWebhookTable ? (
          <>
            <Route index element={<SourceOverviewContainer />} />
            <Route path="details" element={<SimpleObjectDetailsContainer />} />
          </>
        ) : (
          <>
            <Route index element={<SimpleObjectDetailsContainer />} />
          </>
        )}
        {shouldShowColumns && (
          <Route path="columns" element={<ObjectColumns />} />
        )}
        {shouldShowWorkflowGraph && (
          <Route
            path="workflow"
            element={<WorkflowGraph focusedObjectId={id} />}
          />
        )}
        {shouldShowIndexes && (
          <Route path="indexes" element={<ObjectIndexes />} />
        )}
        {shouldShowDataflowVisualizer && (
          <Route path="dataflow-visualizer" element={<DataflowVisualizer />} />
        )}
        <Route path="*" element={<Navigate to="../.." replace />} />
      </Route>
    </SentryRoutes>
  );
};
