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

import { User } from "~/external-library-wrappers/frontegg";
import { SentryRoutes } from "~/sentry";

import AppPasswordsPage from "./AppPasswordsPage";
import MzCliAppPasswordPage from "./MzCliAppPasswordPage";

export const AppPasswordRoutes = ({ user }: { user: User }) => {
  return (
    <SentryRoutes>
      <Route path="/" element={<AppPasswordsPage user={user} />} />
      <Route path="cli" element={<MzCliAppPasswordPage user={user} />} />
    </SentryRoutes>
  );
};
