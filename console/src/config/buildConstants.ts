// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export function getBuildConstants() {
  return {
    basename: __BASENAME__,
    consoleDeploymentMode: __CONSOLE_DEPLOYMENT_MODE__,
    defaultStack: __DEFAULT_STACK__,
    forceOverrideStack: __FORCE_OVERRIDE_STACK__,
    impersonationHostname: __IMPERSONATION_HOSTNAME__,
    mzConsoleImageTag: __MZ_CONSOLE_IMAGE_TAG__,
    sentryEnabled: __SENTRY_ENABLED__ === "true",
    sentryRelease: __SENTRY_RELEASE__,
  };
}

const isBrowser =
  typeof window !== "undefined" && typeof window.document !== "undefined";

export type BuildConstants = ReturnType<typeof getBuildConstants>;

export const buildConstants: BuildConstants = isBrowser
  ? getBuildConstants()
  : {
      basename: "",
      consoleDeploymentMode: "mz-cloud",
      defaultStack: "local",
      forceOverrideStack: undefined,
      impersonationHostname: "",
      mzConsoleImageTag: undefined,
      sentryEnabled: false,
      sentryRelease: "sentry-release",
    };
