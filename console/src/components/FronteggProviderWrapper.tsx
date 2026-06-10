// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { trackSignUpInHubspotActions } from "~/analytics/hubspot";
import LoadingScreen from "~/components/LoadingScreen";
import { RedirectUrlSanitizer } from "~/components/RedirectUrlSanitizer";
import { CloudAppConfig } from "~/config/AppConfig";
import { useAppConfig } from "~/config/useAppConfig";
import {
  FronteggProvider,
  type FronteggProviderProps,
} from "~/external-library-wrappers/frontegg";
import { AUTH_ROUTES } from "~/fronteggRoutes";
import { fronteggAuthPageBackground, getFronteggTheme } from "~/theme";

const buildFronteggProviderProps = (
  appConfig: Readonly<CloudAppConfig>,
): FronteggProviderProps => {
  return {
    localizations: {
      en: {
        loginBox: {
          forgetPassword: {
            submitButtonText: "Reset password",
          },
        },
      },
    },
    contextOptions: { baseUrl: appConfig.fronteggUrl ?? "" },
    events: {
      signUpComplete: trackSignUpInHubspotActions.signUpComplete,
      userVerified: trackSignUpInHubspotActions.userVerified,
    },
    backgroundImage: fronteggAuthPageBackground,
    themeOptions: getFronteggTheme({
      requiresExternalRegistration: appConfig.requiresExternalRegistration,
    }),
    authOptions: {
      enableSessionPerTenant: true,
      routes: {
        loginUrl: AUTH_ROUTES.loginPath,
        logoutUrl: AUTH_ROUTES.logoutPath,
      },
      keepSessionAlive: true,
      enforceRedirectToSameSite: true,
    },
    customLoader: () => <LoadingScreen />,
  };
};

export const FronteggProviderWrapper = ({
  children,
}: React.PropsWithChildren) => {
  const appConfig = useAppConfig();
  if (appConfig.mode === "self-managed" || appConfig.isImpersonating) {
    return children;
  }
  const fronteggProviderProps = buildFronteggProviderProps(appConfig);
  return (
    <RedirectUrlSanitizer>
      <FronteggProvider {...fronteggProviderProps}>{children}</FronteggProvider>
    </RedirectUrlSanitizer>
  );
};
