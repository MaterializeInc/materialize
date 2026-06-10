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

import SourcesList from "~/platform/sources/SourcesList";
import { SentryRoutes } from "~/sentry";

import NewSource from "./create/NewSource";

const SourceRoutes = () => {
  return (
    <SentryRoutes>
      <Route index element={<SourcesList />} />
      <Route path="new">
        <Route index path="*" element={<NewSource />} />
      </Route>
    </SentryRoutes>
  );
};

export default SourceRoutes;
