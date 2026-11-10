// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import { buildFronteggEvents } from "~/components/fronteggEvents";
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
import { getAllowedRedirectOrigins } from "~/utils/redirectUrl";

const buildFronteggProviderProps = (
  appConfig: Readonly<CloudAppConfig>,
  allowedRedirectOrigins: string[],
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
    events: buildFronteggEvents(allowedRedirectOrigins),
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
      // Lets sign-in from the marketing site return to the page it started on.
      allowedRedirectOrigins,
    },
    customLoader: () => <LoadingScreen />,
  };
};

export const FronteggProviderWrapper = ({
  children,
}: React.PropsWithChildren) => {
  const appConfig = useAppConfig();
  const currentStack =
    appConfig.mode === "cloud" ? appConfig.currentStack : undefined;
  // One stable array for both Frontegg and the sanitizer, so the sanitizer's
  // effect doesn't rerun on every render.
  const allowedRedirectOrigins = React.useMemo(
    () => (currentStack ? getAllowedRedirectOrigins(currentStack) : []),
    [currentStack],
  );
  if (appConfig.mode === "self-managed" || appConfig.isImpersonating) {
    return children;
  }
  const fronteggProviderProps = buildFronteggProviderProps(
    appConfig,
    allowedRedirectOrigins,
  );
  return (
    <RedirectUrlSanitizer allowedOrigins={allowedRedirectOrigins}>
      <FronteggProvider {...fronteggProviderProps}>{children}</FronteggProvider>
    </RedirectUrlSanitizer>
  );
};
