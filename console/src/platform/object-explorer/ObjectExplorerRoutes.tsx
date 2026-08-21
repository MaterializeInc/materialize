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

import { useSubscribeToAllNamespacesCollection } from "./allNamespacesCollection";
import { ObjectExplorer } from "./ObjectExplorer";

export const ObjectExplorerRoutes = () => {
  // Feed the objects and namespaces collections the tree reads from. Scoped to
  // this route so the SUBSCRIBEs only run while the object explorer is mounted.
  useSubscribeToAllObjectsCollection();
  useSubscribeToAllNamespacesCollection();
  return (
    <SentryRoutes>
      <Route
        path=":databaseName?/schemas?/:schemaName?/:objectType?/:objectName?/:id?/*"
        element={<ObjectExplorer />}
      />
    </SentryRoutes>
  );
};
