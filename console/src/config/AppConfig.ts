// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { HttpScheme, WebsocketScheme } from "~/api/types";
// HACK (SangJunBak):
// This import path, ~/config/importAppConfig must be in sync with the path specified in Playwright's configuration in playwright.config.ts.
// This is because in the Playwright configuration file, it redirects it to a stub implementation in __mocks__/importAppConfig.ts.
// The stub is necessary because Playwright requires CommonJS (which doesn't support top-level await), while our main appConfig uses ESM with top-level await.
// Once Playwright supports ESM, we can remove this import path and use the actual importAppConfig file.
import { importAppConfig } from "~/config/importAppConfig";
import { SentryConfig } from "~/sentry";
import { CloudRegion } from "~/store/cloudRegions";

import {
  getLaunchDarklyKey,
  getSegmentApiKey,
  getStripePromise,
} from "./apiKeys";
import {
  getCloudGlobalApiUrl,
  getEnvironmentdScheme,
  getEnvironmentdWebsocketScheme,
  getFronteggUrl,
} from "./apiUrls";
import { buildConstants } from "./buildConstants";
import { getCloudRegions } from "./cloudRegions";
import { getConsoleEnvironment } from "./consoleEnvironment";
import { getCurrentStack } from "./currentStack";
import { getEnvironmentdConfig } from "./environment";
import { getImpersonatedEnvironment } from "./impersonation";

const appConfigJson = importAppConfig();
// Message used to indicate that a feature is not supported in the current deployment mode. Usually thrown as an error.
export const NOT_SUPPORTED_MESSAGE = "Not supported in this deployment mode";

function getIsBrowser() {
  return (
    typeof window !== "undefined" && typeof window.document !== "undefined"
  );
}

function getConsoleUrl(isBrowser: boolean) {
  return isBrowser
    ? location
    : new URL(
        process.env.CONSOLE_ADDR || "http://local.dev.materialize.com:3000",
      );
}

type EnvironmentdConfig = {
  environmentdHttpAddress: string;
  regionId: string;
};

export type CloudAppMode = "cloud";

export type SelfManagedAppMode = "self-managed";

export type AppMode = CloudAppMode | SelfManagedAppMode;

type FronteggAuthMode = "Frontegg";
type PasswordAuthMode = "Password";
type NoneAuthMode = "None";
type SaslAuthMode = "Sasl";
type CloudAuthMode = FronteggAuthMode;
export type SelfManagedAuthMode =
  | PasswordAuthMode
  | NoneAuthMode
  | SaslAuthMode;

type AuthMode =
  | FronteggAuthMode
  | PasswordAuthMode
  | NoneAuthMode
  | SaslAuthMode;

interface IBaseAppConfig {
  // Discriminant for the type of app config.
  mode: AppMode;
  // The mode of authentication to use.
  authMode: AuthMode;
  // The URL of the console.
  consoleUrl: Location | URL;
  // The scheme that should be used.
  environmentdScheme: HttpScheme;
  environmentdWebsocketScheme: WebsocketScheme;
  // Whether query retries in react-query are enabled
  reactQueryRetriesEnabled: boolean;
}

export class CloudAppConfig implements IBaseAppConfig {
  mode = "cloud" as const;
  authMode: CloudAuthMode = "Frontegg" as const;

  #isBrowser = getIsBrowser();

  consoleUrl = getConsoleUrl(this.#isBrowser);

  // The URL base path of the console i.e. "internal-console". This is used for impersonation
  // where we need the URL to have 'internal-console' at the base of the path.
  impersonationBasePath = buildConstants.basename;

  // The current cloud stack e.g. "local" or "production".
  currentStack = getCurrentStack({
    hostname: this.consoleUrl.hostname,
    isBrowser: this.#isBrowser,
  });

  // Frontegg API URL for the current environment.
  fronteggUrl = getFronteggUrl(this.currentStack);

  // Region and environment details used during user organization impersonation.
  // Value is null during normal console usage
  impersonation = getImpersonatedEnvironment(
    this.consoleUrl.host,
    buildConstants,
  );

  // Whether the current environment is being impersonated.
  isImpersonating = Boolean(this.impersonation);

  #consoleEnvironment = getConsoleEnvironment({
    hostname: this.consoleUrl.hostname,
    isImpersonating: this.isImpersonating,
  });

  // The global API URL for Materialize Cloud.
  cloudGlobalApiUrl = getCloudGlobalApiUrl({
    stack: this.currentStack,
    isImpersonation: this.isImpersonating,
  });

  // The launchDarkly key for the current environment.
  launchDarklyKey = getLaunchDarklyKey(this.#consoleEnvironment);

  // The Sentry configuration for the current environment.
  sentryConfig: SentryConfig = {
    dsn: "https://13c8b3a8d1e547c9b9493de997b04337@o561021.ingest.sentry.io/5699757",
    environment: this.currentStack,
    release: buildConstants.sentryRelease,
  };

  // The Segment API key for the current environment.
  segmentApiKey = getSegmentApiKey({
    consoleEnv: this.#consoleEnvironment,
    isImpersonating: this.isImpersonating,
  });

  // The stack switcher is a widget that allows users to switch between cloud stacks.
  // Whether the stack switcher is enabled.
  stackSwitcherEnabled =
    !this.isImpersonating && this.#consoleEnvironment !== "production";

  stripePromise = getStripePromise(this.#consoleEnvironment);

  // Whether query retries are enabled. False for test environments.
  // TODO (password-auth): Remove the possibility of defaultStack being "test". Should be able to figure
  // this out via import.meta.vitest
  reactQueryRetriesEnabled = buildConstants.defaultStack !== "test";

  // For impersonation and the Cloud stack switcher, this
  // override is used to specify the regions that are available to the user.
  cloudRegionsOverride = getCloudRegions({
    impersonation: this.impersonation,
  });

  // For impersonation and the Cloud stack switcher, this
  // override is used to specify which http address to target for Materialize's http/ws APIs.
  // Without the override, Console will get this information through the Cloud `/regions` API.
  environmentdConfigOverride: EnvironmentdConfig | null = getEnvironmentdConfig(
    {
      consoleHost: this.consoleUrl.host,
      cloudRegionOverride: this.cloudRegionsOverride?.[0],
      impersonation: this.impersonation,
    },
  );

  // Whether the current environment is a local impersonation.
  isLocalImpersonation = Boolean(buildConstants.impersonationHostname);

  environmentdScheme = getEnvironmentdScheme({
    buildConstants,
    isLocalImpersonation: this.isLocalImpersonation,
  });

  environmentdWebsocketScheme = getEnvironmentdWebsocketScheme({
    buildConstants,
    isLocalImpersonation: this.isLocalImpersonation,
  });

  // Whether the current environment requires user registration outside of the Console. This occurs in production
  // when the Console's 'sign up' button links to the Marketing site.
  requiresExternalRegistration = this.#consoleEnvironment === "production";
}

export class SelfManagedAppConfig implements IBaseAppConfig {
  mode = "self-managed" as const;

  authMode: SelfManagedAuthMode = appConfigJson.auth.mode;

  environmentdScheme = getEnvironmentdScheme({
    buildConstants,
    isLocalImpersonation: false,
  });

  environmentdWebsocketScheme = getEnvironmentdWebsocketScheme({
    buildConstants,
    isLocalImpersonation: false,
  });

  #isBrowser = getIsBrowser();
  consoleUrl = getConsoleUrl(this.#isBrowser);

  // The tag of the Materialize Console image.
  mzConsoleImageTag = buildConstants.mzConsoleImageTag;

  // In self-managed, regions don't exist but we stub it out for convenience.
  // We should remove this for https://github.com/MaterializeInc/console/issues/3812.
  regionsStub: CloudRegion[] = [
    {
      provider: "local",
      region: "flexible-deployment",
      regionApiUrl: "",
    },
  ];

  // Used to specify which http address to target for Materialize's http/ws APIs.
  environmentdConfig: EnvironmentdConfig = {
    // We use the console's host (usually localhost:3000) to target the environmentd.
    // The reason we can do this is for self managed deployments, we're hosting on an nginx server
    // with a proxy that forwards requests from :3000/api to :${MZ_ENDPOINT}/api.
    // This nginx config is defined in `misc/docker/nginx.conf.template`.
    environmentdHttpAddress: this.consoleUrl.host,
    regionId: `${this.regionsStub[0].provider}/${this.regionsStub[0].region}`,
  };

  // Whether query retries are enabled.
  // TODO (password-auth): We should be using import.meta.vitest to determine this.
  reactQueryRetriesEnabled = true;
}

/**
 * Global application configuration singleton.
 * This is intentionally a global singleton because:
 * 1. It's determined at build/startup time and never changes
 * 2. It's needed throughout the application, including non-React code
 * 3. It represents truly global settings that affect the entire application
 */
export const appConfig = Object.freeze(
  buildConstants.consoleDeploymentMode === "flexible-deployment"
    ? new SelfManagedAppConfig()
    : new CloudAppConfig(),
);

export type AppConfig = typeof appConfig;
