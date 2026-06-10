// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React, { useCallback } from "react";
import { useNavigate } from "react-router-dom";

import LoadingScreen from "~/components/LoadingScreen";
import { useAppConfig } from "~/config/useAppConfig";
import {
  AuthProvider,
  useOidcManagerQuery,
} from "~/external-library-wrappers/oidc";

export const OidcProviderWrapper = ({ children }: React.PropsWithChildren) => {
  const navigate = useNavigate();
  const appConfig = useAppConfig();

  const isOidc =
    appConfig.mode === "self-managed" && appConfig.authMode === "Oidc";

  const { data: oidcManager, isLoading, error } = useOidcManagerQuery();

  const onSigninCallback = useCallback(() => {
    navigate("/", { replace: true });
  }, [navigate]);

  if (!isOidc) {
    return children;
  }

  if (isLoading) {
    return <LoadingScreen />;
  }

  // If OIDC isn't ready, render children without AuthProvider so password
  // sign-in still works. Components that need OIDC must guard against
  // `useAuth()` returning undefined.
  if (error || !oidcManager) {
    return children;
  }

  return (
    <AuthProvider
      userManager={oidcManager.getUserManager()}
      onSigninCallback={onSigninCallback}
    >
      {children}
    </AuthProvider>
  );
};
