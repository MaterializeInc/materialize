// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Route } from "react-router-dom";

import { SentryRoutes } from "~/sentry";

import ConnectorMonitorPage from "./ConnectorMonitorPage";
import DataflowVisualizerPage from "./DataflowVisualizerPage";
import WorkflowGraphPage from "./WorkflowGraphPage";
import WorksheetPage from "./WorksheetPage";

const WorksheetRoutes = () => (
  <SentryRoutes>
    <Route path="/" element={<WorksheetPage />}>
      <Route index element={null} />
      <Route path="monitor/:id" element={<ConnectorMonitorPage />} />
      <Route path="monitor/:id/errors" element={<ConnectorMonitorPage />} />
      <Route path="workflow/:id" element={<WorkflowGraphPage />} />
      <Route path="dataflow/:id" element={<DataflowVisualizerPage />} />
    </Route>
  </SentryRoutes>
);

export default WorksheetRoutes;
