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
import { useSubscribeToAllObjectsCollection } from "~/store/allObjectsCollection";
import { useSubscribeToWorkflowGraphEdgesCollection } from "~/store/workflowGraphEdgesCollection";

import { useSubscribeToAllNamespacesCollection } from "./allNamespacesCollection";
import { ObjectExplorer } from "./ObjectExplorer";

export const ObjectExplorerRoutes = () => {
  // Feed the collections the tree and the workflow graph read from. Objects
  // reuse the app-wide `allObjects` subscribe. Namespaces and graph edges open
  // their own SUBSCRIBEs, scoped to this route so they run while the object
  // explorer is mounted, and opening a graph sends none.
  useSubscribeToAllObjectsCollection();
  useSubscribeToAllNamespacesCollection();
  useSubscribeToWorkflowGraphEdgesCollection();
  return (
    <SentryRoutes>
      <Route
        path=":databaseName?/schemas?/:schemaName?/:objectType?/:objectName?/:id?/*"
        element={<ObjectExplorer />}
      />
    </SentryRoutes>
  );
};
