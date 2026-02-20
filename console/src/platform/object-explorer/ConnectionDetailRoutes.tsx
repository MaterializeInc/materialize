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

import { SentryRoutes } from "~/sentry";

import { ConnectionDetails } from "./ConnectionDetails";
import { SchemaObjectHeader } from "./SchemaObjectHeader";
import { useSchemaObjectParams } from "./useSchemaObjectParams";
import { useToastIfObjectNotExtant } from "./useToastIfObjectNotExtant";

const CONNECTION_TABS = [
  {
    label: "Details",
    href: "..",
    end: true,
  },
];

export const ConnectionDetailRoutes = () => {
  const objectRouteParams = useSchemaObjectParams();
  useToastIfObjectNotExtant();
  return (
    <SentryRoutes>
      <Route
        element={
          <>
            <SchemaObjectHeader
              objectType="connection"
              tabStripItems={CONNECTION_TABS}
            />
            <Outlet />
          </>
        }
      >
        <Route index element={<ConnectionDetails {...objectRouteParams} />} />
        <Route path="*" element={<Navigate to="../.." replace />} />
      </Route>
    </SentryRoutes>
  );
};
