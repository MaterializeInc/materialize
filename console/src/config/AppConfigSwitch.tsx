// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";

import {
  type AuthActions,
  type AuthState,
  useAuth,
  useAuthActions,
  useAuthUser,
  type User,
} from "~/external-library-wrappers/frontegg";
import {
  useAuth as useOidcAuth,
  useOidcManagerQuery,
} from "~/external-library-wrappers/oidc";

import { CloudAppConfig, SelfManagedAppConfig } from "./AppConfig";
import { useAppConfig } from "./useAppConfig";

type CloudImpersonationRuntimeConfig = {
  isImpersonating: true;
};

type CloudFronteggRuntimeConfig = {
  isImpersonating: false;
  user: User;
  auth: AuthState;
  authActions: AuthActions;
};

export type CloudRuntimeConfig =
  | CloudFronteggRuntimeConfig
  | CloudImpersonationRuntimeConfig;

type SelfManagedOidcAvailableRuntimeConfig = {
  isOidcAvailable: true;
  auth: NonNullable<ReturnType<typeof useOidcAuth>>;
};

type SelfManagedOidcUnavailableRuntimeConfig = {
  isOidcAvailable: false;
};

export type SelfManagedRuntimeConfig =
  | SelfManagedOidcAvailableRuntimeConfig
  | SelfManagedOidcUnavailableRuntimeConfig;

type CloudConfigElementRenderProps = {
  appConfig: Readonly<CloudAppConfig>;
  runtimeConfig: CloudRuntimeConfig;
};

type SelfManagedConfigElementRenderProps = {
  appConfig: Readonly<SelfManagedAppConfig>;
  runtimeConfig: SelfManagedRuntimeConfig;
};

type CloudConfigElementFunction = (
  props: CloudConfigElementRenderProps,
) => React.ReactNode;

type CloudConfigElement = React.ReactNode | CloudConfigElementFunction;
type SelfManagedConfigElementFunction = (
  props: SelfManagedConfigElementRenderProps,
) => React.ReactNode;

type SelfManagedConfigElement =
  | React.ReactNode
  | SelfManagedConfigElementFunction;

const CloudConfigElementWrapper = ({
  cloudAppConfig,
  cloudConfigElement,
}: {
  cloudAppConfig: Readonly<CloudAppConfig>;
  cloudConfigElement: CloudConfigElementFunction;
}) => {
  const user = useAuthUser();
  const auth = useAuth();
  const authActions = useAuthActions();
  return cloudConfigElement({
    appConfig: cloudAppConfig,
    runtimeConfig: {
      isImpersonating: false,
      user,
      auth,
      authActions,
    },
  });
};

// A wrapper for the app with Cloud config but impersonation enabled.
// This is separate from the CloudConfigElementWrapper because we don't want to
// using the useAuthUser hook in the impersonation case would throw an error.
const CloudImpersonationConfigElementWrapper = ({
  cloudAppConfig,
  cloudConfigElement,
}: {
  cloudAppConfig: Readonly<CloudAppConfig>;
  cloudConfigElement: CloudConfigElementFunction;
}) => {
  return cloudConfigElement({
    appConfig: cloudAppConfig,
    runtimeConfig: {
      isImpersonating: true,
    },
  });
};

// Only mounted when the OIDC manager has initialized, which implies
// OidcProviderWrapper has mounted an AuthProvider in scope.
const SelfManagedOidcAvailableConfigElementWrapper = ({
  selfManagedAppConfig,
  selfManagedConfigElement,
}: {
  selfManagedAppConfig: Readonly<SelfManagedAppConfig>;
  selfManagedConfigElement: SelfManagedConfigElementFunction;
}) => {
  const auth = useOidcAuth();

  return selfManagedConfigElement({
    appConfig: selfManagedAppConfig,
    runtimeConfig: { isOidcAvailable: true, auth },
  });
};

const SelfManagedConfigElementWrapper = ({
  selfManagedAppConfig,
  selfManagedConfigElement,
}: {
  selfManagedAppConfig: Readonly<SelfManagedAppConfig>;
  selfManagedConfigElement: SelfManagedConfigElementFunction;
}) => {
  const { data: oidcManager } = useOidcManagerQuery();

  if (!oidcManager) {
    return selfManagedConfigElement({
      appConfig: selfManagedAppConfig,
      runtimeConfig: { isOidcAvailable: false },
    });
  }

  return (
    <SelfManagedOidcAvailableConfigElementWrapper
      selfManagedAppConfig={selfManagedAppConfig}
      selfManagedConfigElement={selfManagedConfigElement}
    />
  );
};

// A component that controls which component to render based on the deployment mode.
// This is used to avoid having to do a discriminant check throughout the application.
//
// @param cloudConfigElement - A render prop or React component that is called/returned if the app is in cloud mode.
// @param selfManagedConfigElement - A render prop or React component that is called/returned if the app is in self-managed mode.
// Note: Because of the use of useAuthUser, users will be redirected to the login page if they are not logged in. Thus we
// should only use this component within the AuthenticatedRoutes component.
//
export const AppConfigSwitch = ({
  cloudConfigElement,
  selfManagedConfigElement,
}: {
  cloudConfigElement?: CloudConfigElement;
  selfManagedConfigElement?: SelfManagedConfigElement;
  shouldAutoLogout?: boolean;
}) => {
  const appConfig = useAppConfig();

  if (appConfig.mode === "cloud") {
    if (typeof cloudConfigElement === "function") {
      if (appConfig.isImpersonating) {
        return (
          <CloudImpersonationConfigElementWrapper
            cloudAppConfig={appConfig}
            cloudConfigElement={cloudConfigElement}
          />
        );
      }
      return (
        <CloudConfigElementWrapper
          cloudAppConfig={appConfig}
          cloudConfigElement={cloudConfigElement}
        />
      );
    }
    return cloudConfigElement;
  }

  if (typeof selfManagedConfigElement === "function") {
    return (
      <SelfManagedConfigElementWrapper
        selfManagedAppConfig={appConfig}
        selfManagedConfigElement={selfManagedConfigElement}
      />
    );
  }

  return selfManagedConfigElement;
};
