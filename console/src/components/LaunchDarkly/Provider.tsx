// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { LDContext } from "launchdarkly-js-client-sdk";
import React from "react";

import { AppConfigSwitch } from "~/config/AppConfigSwitch";
import {
  buildLdContext,
  buildLdContextForImpersonation,
  useLaunchDarklyClient,
} from "~/hooks/useLaunchDarklyClient";

import { LaunchDarklyContext } from "./Context";

const CloudLaunchDarklyProvider = ({
  children,
  launchDarklyKey,
  ldContext,
}: {
  children?: React.ReactNode;
  launchDarklyKey: string;
  ldContext: LDContext;
}) => {
  const ldClient = useLaunchDarklyClient(launchDarklyKey, ldContext);

  return (
    <LaunchDarklyContext.Provider value={{ client: ldClient }}>
      {children}
    </LaunchDarklyContext.Provider>
  );
};

/**
 * Provides our LaunchDarklyContext, which holds an LDClient instance. Used by the
 * useFlags hook.
 */
export const LaunchDarklyProvider = (props: { children?: React.ReactNode }) => (
  <AppConfigSwitch
    cloudConfigElement={({ appConfig, runtimeConfig }) => {
      const ldContext = runtimeConfig.isImpersonating
        ? buildLdContextForImpersonation(
            appConfig.impersonation?.organizationId ?? "",
          )
        : buildLdContext(runtimeConfig.user);
      return (
        <CloudLaunchDarklyProvider
          {...props}
          launchDarklyKey={appConfig.launchDarklyKey}
          ldContext={ldContext}
        />
      );
    }}
  />
);
