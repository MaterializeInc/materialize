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
import LoadingScreen from "~/components/LoadingScreen";
import { type SelfManagedAppConfig } from "~/config/AppConfig";
import { useAppConfig } from "~/config/useAppConfig";
import { useIsAuthenticated } from "~/external-library-wrappers/frontegg";
import { hasAuthParams, useAuth } from "~/external-library-wrappers/oidc";
import { AUTH_ROUTES } from "~/fronteggRoutes";
import { AuthenticatedRoutes } from "~/platform/AuthenticatedRoutes";
import { SentryRoutes } from "~/sentry";

import { Login } from "./auth/Login";

const OidcAuthGuard = ({ children }: React.PropsWithChildren) => {
  const auth = useAuth();

  if (auth.isLoading || hasAuthParams()) {
    return <LoadingScreen />;
  }

  if (!auth.isAuthenticated) {
    return <Navigate to={LOGIN_PATH} replace />;
  }

  return children;
};

const SelfManagedRoutes = ({
  appConfig,
}: {
  appConfig: Readonly<SelfManagedAppConfig>;
}) => {
  const isOidc = appConfig.authMode === "Oidc";

  return (
    <SentryRoutes>
      {(appConfig.authMode === "Password" ||
        appConfig.authMode === "Sasl" ||
        isOidc) && <Route path={LOGIN_PATH} element={<Login />} />}
      {isOidc && <Route path="/auth/callback" element={<LoadingScreen />} />}
      <Route
        path="*"
        element={
          isOidc ? (
            <OidcAuthGuard>
              <AuthenticatedRoutes />
            </OidcAuthGuard>
          ) : (
            <AuthenticatedRoutes />
          )
        }
      />
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
