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

import { LOGIN_PATH } from "~/api/materialize/auth";
import { LaunchDarklyProvider } from "~/components/LaunchDarkly";
import { type SelfManagedAppConfig } from "~/config/AppConfig";
import { useAppConfig } from "~/config/useAppConfig";
import { useIsAuthenticated } from "~/external-library-wrappers/frontegg";
import { AUTH_ROUTES } from "~/fronteggRoutes";
import { AuthenticatedRoutes } from "~/platform/AuthenticatedRoutes";
import { SentryRoutes } from "~/sentry";

import { Login } from "./auth/Login";

const SelfManagedRoutes = ({
  appConfig,
}: {
  appConfig: Readonly<SelfManagedAppConfig>;
}) => {
  return (
    <SentryRoutes>
      {(appConfig.authMode === "Password" || appConfig.authMode === "Sasl") && (
        <Route path={LOGIN_PATH} element={<Login />} />
      )}
      <Route path="*" element={<AuthenticatedRoutes />} />
    </SentryRoutes>
  );
};

const CloudAuthenticatedRoutes = () => {
  return (
    <LaunchDarklyProvider>
      <AuthenticatedRoutes />
    </LaunchDarklyProvider>
  );
};

const CloudFronteggAuthenticatedRoutes = () => {
  const isAuthenticated = useIsAuthenticated();

  if (!isAuthenticated) {
    const fullPath = location.pathname + location.search + location.hash;
    const redirectUrl = encodeURIComponent(fullPath);
    return (
      <Navigate to={`${AUTH_ROUTES.loginPath}?redirectUrl=${redirectUrl}`} />
    );
  }

  return <CloudAuthenticatedRoutes />;
};

export const UnauthenticatedRoutes = () => {
  const appConfig = useAppConfig();

  if (appConfig.mode === "self-managed") {
    return <SelfManagedRoutes appConfig={appConfig} />;
  }
  // We assume impersonation users are already authenticated before they load the Console.

  if (appConfig.mode === "cloud" && appConfig.isImpersonating) {
    return <CloudAuthenticatedRoutes />;
  }

  return <CloudFronteggAuthenticatedRoutes />;
};
