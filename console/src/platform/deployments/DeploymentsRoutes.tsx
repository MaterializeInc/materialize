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

import { SentryRoutes } from "~/sentry";

import DeploymentDetail from "./DeploymentDetail";
import DeploymentsList from "./DeploymentsList";

const DeploymentsRoutes = () => (
  <SentryRoutes>
    <Route index element={<DeploymentsList />} />
    <Route path=":deployId" element={<DeploymentDetail />} />
    <Route path="*" element={<Navigate to="." replace />} />
  </SentryRoutes>
);

export default DeploymentsRoutes;
