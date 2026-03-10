// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React, { useCallback, useMemo } from "react";
import { useNavigate } from "react-router-dom";

import { useAppConfig } from "~/config/useAppConfig";
import {
  AuthProvider,
  initOidcUserManager,
} from "~/external-library-wrappers/oidc";

export const OidcProviderWrapper = ({ children }: React.PropsWithChildren) => {
  const appConfig = useAppConfig();
  const navigate = useNavigate();

  const userManager = useMemo(() => {
    if (appConfig.mode !== "self-managed" || appConfig.authMode !== "Oidc") {
      return null;
    }
    return appConfig.oidcConfig
      ? initOidcUserManager(appConfig.oidcConfig)
      : null;
  }, [appConfig]);

  const onSigninCallback = useCallback(() => {
    // After the OIDC provider redirects back, navigate to "/" to clear
    // the callback URL params and let the auth guard take over.
    navigate("/", { replace: true });
  }, [navigate]);

  if (!userManager) {
    return children;
  }

  return (
    <AuthProvider userManager={userManager} onSigninCallback={onSigninCallback}>
      {children}
    </AuthProvider>
  );
};
