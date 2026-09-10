// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useQuery } from "@tanstack/react-query";
import React from "react";
import { Navigate, Route, useSearchParams } from "react-router-dom";

import { hasActiveSession, LOGIN_PATH } from "~/api/materialize/auth";
import { LaunchDarklyProvider } from "~/components/LaunchDarkly";
import LoadingScreen from "~/components/LoadingScreen";
import { type SelfManagedAppConfig } from "~/config/AppConfig";
import { useAppConfig } from "~/config/useAppConfig";
import { useIsAuthenticated } from "~/external-library-wrappers/frontegg";
import {
  hasAuthParams,
  useOidcManagerQuery,
} from "~/external-library-wrappers/oidc";
import { AUTH_ROUTES } from "~/fronteggRoutes";
import { AuthenticatedRoutes } from "~/platform/AuthenticatedRoutes";
import { SentryRoutes } from "~/sentry";

import { Login } from "./auth/Login";
import { OidcCallback } from "./auth/OidcCallback";
import { OryLoginPage } from "./auth/ory/OryLoginPage";

// Redirect already-signed-in users off the login page. The password session
// cookie is httpOnly, so probe the server; a live OIDC token skips the probe.
export const LoginRoute = () => {
  const [searchParams] = useSearchParams();
  // Ory marks a request it sent here to be authenticated: `flow` from Kratos,
  // `login_challenge` from Hydra on behalf of an OAuth2 client. Either means
  // this page is the identity provider's login UI for the duration of that
  // request rather than the console's own front door.
  const isOryFlow =
    searchParams.has("flow") || searchParams.has("login_challenge");

  const { data: oidcManager } = useOidcManagerQuery();
  const hasOidcToken = Boolean(oidcManager?.getIdToken());

  const { data: hasCookieSession } = useQuery({
    queryKey: ["hasActiveSession"],
    queryFn: hasActiveSession,
    enabled: !hasOidcToken && !isOryFlow,
    staleTime: Infinity,
    retry: false,
  });

  // Checked before the signed-in redirect below: a client may ask someone who
  // already has a session to re-authenticate, and sending them to the app
  // instead would strand the flow with no way to finish.
  if (isOryFlow) {
    return <OryLoginPage />;
  }
  if (hasOidcToken || hasCookieSession) {
    return <Navigate to="/" replace />;
  }
  return <Login />;
};

const OidcAuthGuard = ({ children }: React.PropsWithChildren) => {
  const { isLoading, data: auth } = useOidcManagerQuery();

  // OIDC initialization failed — `OidcProviderWrapper` rendered us without
  // an `AuthProvider` so password sign-in still works. Skip the OIDC checks
  // and let the user reach the app via their password session cookie.
  if (!auth) return <>{children}</>;

  if (isLoading || hasAuthParams()) {
    return <LoadingScreen />;
  }

  // Don't redirect — the user may have a valid password session cookie.
  // The 401 redirect middleware handles expired sessions.
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
        isOidc) && <Route path={LOGIN_PATH} element={<LoginRoute />} />}
      {isOidc && <Route path="/auth/callback" element={<OidcCallback />} />}
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
