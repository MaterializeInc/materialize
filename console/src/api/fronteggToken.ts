// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";

import { ContextHolder } from "~/external-library-wrappers/frontegg";

/**
 * Returns the current Frontegg access token.
 * Usually we can get the access token from the `useAuth` hook,
 * but for our middlewares that aren't in the context of
 * React, we want to get it straight from the Frontegg
 * Redux store which is available via `ContextHolder`.
 */
export function getAccessToken() {
  // Get the access token from Frontegg's context holder
  const accessToken = ContextHolder.for("default").getAccessToken();

  // The token will most likely always exist since this function
  // will be called when logged in. If not, we should alert Sentry.
  if (!accessToken) {
    Sentry.addBreadcrumb({
      level: "error",
      category: "auth",
      message: "Failed to refresh auth token",
    });
  }
  return accessToken;
}
