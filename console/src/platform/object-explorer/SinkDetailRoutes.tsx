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

import { AppErrorBoundary } from "~/components/AppErrorBoundary";
import { LoadingContainer } from "~/components/LoadingContainer";
import WorkflowGraph from "~/components/WorkflowGraph/WorkflowGraph";
import { Tab } from "~/layouts/BaseLayout";
import { useSinkList } from "~/platform/sinks/queries";
import SinkErrors from "~/platform/sinks/SinkErrors";
import SinkOverview from "~/platform/sinks/SinkOverview";
import { SentryRoutes } from "~/sentry";

import { SchemaObjectHeader } from "./SchemaObjectHeader";
import { SimpleObjectDetailsContainer } from "./SimpleObjectDetailRoutes";
import { useSchemaObjectParams } from "./useSchemaObjectParams";
import { useToastIfObjectNotExtant } from "./useToastIfObjectNotExtant";

const WORKFLOW_TAB = {
  label: "Workflow",
  href: `../workflow`,
};

const OVERVIEW_TAB = {
  label: "Overview",
  href: "..",
  end: true,
};

const DETAILS_TAB = {
  label: "Details",
  href: "../details",
  end: true,
};

const ERRORS_TAB = {
  label: "Errors",
  href: `../errors`,
};

function useSinkOrRedirect() {
  const { databaseName, schemaName, objectName } = useSchemaObjectParams();
  const navigate = useNavigate();
  const { data } = useSinkList();
  const sink = data.rows.find(
    (s) =>
      s.databaseName === databaseName &&
      s.schemaName === schemaName &&
      s.name === objectName,
  );
  React.useEffect(() => {
    if (!sink) {
      navigate("..");
    }
  }, [navigate, sink]);
  if (!sink) return null;
  return sink;
}

export const SinkOverviewContainer = () => {
  const sink = useSinkOrRedirect();
  if (!sink) return null;

  return (
    <AppErrorBoundary message="An error occurred loading sink details.">
      <React.Suspense fallback={<LoadingContainer />}>
        <SinkOverview sink={sink} />
      </React.Suspense>
    </AppErrorBoundary>
  );
};

export const SinkErrorsContainer = () => {
  const sink = useSinkOrRedirect();
  if (!sink) return null;

  return (
    <AppErrorBoundary message="An error occurred loading sink errors.">
      <React.Suspense fallback={<LoadingContainer />}>
        <SinkErrors sink={sink} />
      </React.Suspense>
    </AppErrorBoundary>
  );
};

export const SinkDetailRoutes = () => {
  const { id } = useSchemaObjectParams();
  useToastIfObjectNotExtant();

  const tabStripItems: Tab[] = [
    OVERVIEW_TAB,
    DETAILS_TAB,
    ERRORS_TAB,
    WORKFLOW_TAB,
  ];

  return (
    <SentryRoutes>
      <Route
        element={
          <>
            <SchemaObjectHeader
              tabStripItems={tabStripItems}
              objectType="sink"
            />
            <Outlet />
          </>
        }
      >
        <Route index element={<SinkOverviewContainer />} />
        <Route path="details" element={<SimpleObjectDetailsContainer />} />
        <Route path="errors" element={<SinkErrorsContainer />} />
        <Route
          path="workflow"
          element={<WorkflowGraph focusedObjectId={id} />}
        />
        <Route path="*" element={<Navigate to="../.." replace />} />
      </Route>
    </SentryRoutes>
  );
};
