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

import { LOGIN_ERROR_STORAGE_KEY, LOGIN_PATH } from "~/api/materialize/auth";
import LoadingScreen from "~/components/LoadingScreen";
import { useAuth } from "~/external-library-wrappers/oidc";
import storageAvailable from "~/utils/storageAvailable";

// Forwards IdP callback errors to the login page so all sign-in errors
// render in one place.
export const OidcCallback = () => {
  const auth = useAuth();
  // OIDC initialization failed — there's nothing to complete here, so send
  // the user back to the login page.
  if (!auth) {
    return <Navigate to={LOGIN_PATH} replace />;
  }
  if (auth.error) {
    const message = auth.error.message?.trim() || "Sign-in failed";
    if (storageAvailable("sessionStorage")) {
      window.sessionStorage.setItem(LOGIN_ERROR_STORAGE_KEY, message);
    }
    return <Navigate to={LOGIN_PATH} replace />;
  }
  return <LoadingScreen />;
};
