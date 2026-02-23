// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Navigate, Route } from "react-router-dom";

import ErrorBox from "~/components/ErrorBox";
import { LoadingContainer } from "~/components/LoadingContainer";
import { SentryRoutes } from "~/sentry";
import { useAllObjects } from "~/store/allObjects";

import { ConnectionDetailRoutes } from "./ConnectionDetailRoutes";
import { DatabaseDetails } from "./DatabaseDetails";
import { SchemaDetails } from "./SchemaDetails";
import { SimpleObjectDetailRoutes } from "./SimpleObjectDetailRoutes";
import { SinkDetailRoutes } from "./SinkDetailRoutes";
import { SourceDetailRoutes } from "./SourceDetailRoutes";

export const ObjectExplorerDetailRoutes = () => {
  const { snapshotComplete: objectSnapshotComplete, isError: isObjectError } =
    useAllObjects();
  const objectPath = ":objectName/:id";

  if (isObjectError) {
    return <ErrorBox />;
  }
  if (!objectSnapshotComplete) {
    return <LoadingContainer />;
  }

  return (
    <SentryRoutes>
      <Route path=":databaseName" element={<DatabaseDetails />} />
      <Route path=":databaseName/schemas/:schemaName">
        <Route index element={<SchemaDetails />} />
        <Route path={`sinks/${objectPath}`}>
          <Route index path="*" element={<SinkDetailRoutes />} />
        </Route>
        <Route path={`sources/${objectPath}`}>
          <Route index path="*" element={<SourceDetailRoutes />} />
        </Route>
        <Route path={`connections/${objectPath}`}>
          <Route index path="*" element={<ConnectionDetailRoutes />} />
        </Route>
        {/* Minimal catch-all for simple objects */}
        <Route path={`:objectType/${objectPath}`}>
          <Route index path="*" element={<SimpleObjectDetailRoutes />} />
        </Route>
        <Route path="*" element={<Navigate to=".." replace />} />
      </Route>
      <Route path="*" element={<Navigate to=".." replace />} />
    </SentryRoutes>
  );
};
