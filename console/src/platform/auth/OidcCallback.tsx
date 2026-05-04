// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Navigate } from "react-router-dom";

import { LOGIN_ERROR_PARAM, LOGIN_PATH } from "~/api/materialize/auth";
import LoadingScreen from "~/components/LoadingScreen";
import { useAuth } from "~/external-library-wrappers/oidc";

// Forwards IdP callback errors to the login page so all sign-in errors
// render in one place.
export const OidcCallback = () => {
  const auth = useAuth();
  if (auth.error) {
    const message = auth.error.message?.trim() || "Sign-in failed";
    const params = new URLSearchParams({ [LOGIN_ERROR_PARAM]: message });
    return <Navigate to={`${LOGIN_PATH}?${params.toString()}`} replace />;
  }
  return <LoadingScreen />;
};
