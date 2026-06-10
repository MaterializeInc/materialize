// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Navigate, Outlet, Route, useNavigate } from "react-router-dom";

import { Source } from "~/api/materialize/source/sourceList";
import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import WorkflowGraph from "~/components/WorkflowGraph/WorkflowGraph";
import { Tab } from "~/layouts/BaseLayout";
import { useSourcesList } from "~/platform/sources/queries";
import SourceErrors from "~/platform/sources/SourceErrors";
import { SourceOverview } from "~/platform/sources/SourceOverview";
import SourceTables from "~/platform/sources/SourceTables";
import { SentryRoutes } from "~/sentry";
import { useAllObjects } from "~/store/allObjects";

import { ObjectColumns } from "./ObjectColumns";
import { ObjectIndexes } from "./ObjectIndexes";
import { SchemaObjectHeader } from "./SchemaObjectHeader";
import { SimpleObjectDetailsContainer } from "./SimpleObjectDetailRoutes";
import { useSchemaObjectParams } from "./useSchemaObjectParams";
import { useToastIfObjectNotExtant } from "./useToastIfObjectNotExtant";

export const SourceOverviewContainer = () => {
  return (
    <AppErrorBoundary message="An error occurred loading object details.">
      <React.Suspense fallback={<LoadingContainer />}>
        <SourceOrRedirect>
          {(source) => <SourceOverview source={source} />}
        </SourceOrRedirect>
      </React.Suspense>
    </AppErrorBoundary>
  );
};

export const SourceErrorsContainer = () => {
  return (
    <AppErrorBoundary message="An error occurred loading source errors.">
      <React.Suspense fallback={<LoadingContainer />}>
        <SourceOrRedirect>
          {(source) => <SourceErrors source={source} />}
        </SourceOrRedirect>
      </React.Suspense>
    </AppErrorBoundary>
  );
};

export const SourceOrRedirect = (props: {
  children: (source: Source) => React.ReactNode;
}) => {
  const { databaseName, schemaName, objectName } = useSchemaObjectParams();
  const navigate = useNavigate();
  const { data } = useSourcesList({});
  const source = data.rows.find(
    (s) =>
      s.databaseName === databaseName &&
      s.schemaName === schemaName &&
      s.name === objectName,
  );
  React.useEffect(() => {
    if (!source) {
      navigate("../..");
    }
  }, [navigate, source]);
  if (!source) return null;

  return props.children(source);
};

export const SourceTablesContainer = () => {
  return (
    <AppErrorBoundary message="An error occurred loading tables.">
      <React.Suspense fallback={<LoadingContainer />}>
        <SourceOrRedirect>
          {(source) => <SourceTables sourceId={source.id} />}
        </SourceOrRedirect>
      </React.Suspense>
    </AppErrorBoundary>
  );
};

const COLUMN_TAB = {
  label: "Columns",
  href: `../columns`,
};

const WORKFLOW_TAB = {
  label: "Workflow",
  href: `../workflow`,
};

const INDEX_TAB = {
  label: "Indexes",
  href: `../indexes`,
};

const DETAILS_TAB = {
  label: "Details",
  href: `..`,
  end: true,
};

export const SourceDetailRoutes = () => {
  const { id } = useSchemaObjectParams();
  useToastIfObjectNotExtant();
  const { data: objects } = useAllObjects();
  const object = objects.find((o) => o.id === id);

  const isSubsource = object?.sourceType === "subsource";
  const isProgressSource = object?.sourceType === "progress";
  const isBuiltinSource =
    object?.sourceType === "source" || object?.sourceType === "log";

  let tabStripItems: Tab[] = [];

  if (isProgressSource || isBuiltinSource) {
    tabStripItems = [DETAILS_TAB, COLUMN_TAB, INDEX_TAB];
  } else if (isSubsource) {
    tabStripItems = [DETAILS_TAB, WORKFLOW_TAB, COLUMN_TAB, INDEX_TAB];
  } else if (object?.objectType === "source") {
    tabStripItems = [
      {
        label: "Overview",
        href: `..`,
        end: true,
      },
      {
        label: "Subsources",
        href: `../tables`,
      },
      {
        label: "Details",
        href: `../details`,
        end: true,
      },
      {
        label: "Errors",
        href: `../errors`,
      },
      WORKFLOW_TAB,
      INDEX_TAB,
    ];
  }
  return (
    <SentryRoutes>
      <Route
        element={
          <>
            <SchemaObjectHeader
              tabStripItems={tabStripItems}
              objectType="source"
            />
            <Outlet />
          </>
        }
      >
        {isProgressSource ? (
          <>
            <Route index element={<SimpleObjectDetailsContainer />} />
            <Route path="columns" element={<ObjectColumns />} />
          </>
        ) : isBuiltinSource || isSubsource ? (
          <>
            <Route index element={<SimpleObjectDetailsContainer />} />
            <Route path="columns" element={<ObjectColumns />} />
            <Route
              path="workflow"
              element={<WorkflowGraph focusedObjectId={id} />}
            />
          </>
        ) : (
          <>
            <Route index element={<SourceOverviewContainer />} />
            <Route path="details" element={<SimpleObjectDetailsContainer />} />
            <Route path="columns" element={<ObjectColumns />} />
            <Route
              path="workflow"
              element={<WorkflowGraph focusedObjectId={id} />}
            />
            <Route path="errors" element={<SourceErrorsContainer />} />
            <Route path="tables" element={<SourceTablesContainer />} />
          </>
        )}
        <Route path="indexes" element={<ObjectIndexes />} />

        <Route path="*" element={<Navigate to="../.." replace />} />
      </Route>
    </SentryRoutes>
  );
};
