// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";

import {
  AppConfig,
  appConfig as appConfigSingleton,
  CloudAppConfig,
  CloudAppMode,
  SelfManagedAppConfig,
  SelfManagedAppMode,
  SelfManagedAuthMode,
} from "~/config/AppConfig";
import { ContextHolder } from "~/external-library-wrappers/frontegg";

import { logoutAndRedirect } from "./materialize/auth";
import {
  HttpScheme,
  MaterializeAuthConfig,
  PasswordAuthConfig,
  TokenAuthConfig,
  WebsocketScheme,
} from "./types";

export type Fetch = typeof fetch;

export type Middleware = (next: Fetch) => Fetch;

// HACK (SangJunBak): This is a hack to ensure that the fetch function is always the most up to date reference.
// During testing, MSW (Mock Service Worker) intercepts and replaces the global fetch function.
// However, storing a direct reference to fetch would capture the original function, which becomes stale after MSW's replacement.
// By wrapping fetch in an anonymous function, we ensure we always access the current version
// of fetch through closure, allowing MSW to properly intercept and handle requests during tests.
const globalFetch = (...req: Parameters<Fetch>) => fetch(...req);

function buildConsoleVersionHeaders(
  currentConsoleVersion: string,
): Record<string, string> {
  return currentConsoleVersion && currentConsoleVersion.length > 0
    ? {
        "X-MATERIALIZE-VERSION": currentConsoleVersion,
      }
    : {};
}

// User and password used to connect to Materialize during flexible deployment mode.
// These are required fields for the Websocket API for self-managed without auth.
export const FLEXIBLE_DEPLOYMENT_USER = {
  user: "materialize",
  password: "",
};

/**
 * Composes multiple middleware functions into a single middleware
 * @param middlewares Array of middleware functions to compose
 * @returns A single composed middleware function
 */
function composeMiddleware(...middlewares: Middleware[]): Middleware {
  return (finalFetch: Fetch): Fetch => {
    return middlewares.reduceRight(
      (next, middleware) => middleware(next),
      finalFetch,
    );
  };
}

/**
 * Creates a new fetch function with the given middlewares applied
 * @param fetch The base fetch function to transform
 * @param middlewares Array of middleware functions to apply
 * @returns A new fetch function with all middlewares applied
 */
export function withMiddleware(
  fetch: Fetch,
  ...middlewares: Middleware[]
): Fetch {
  return composeMiddleware(...middlewares)(fetch);
}

/**
 * Creates a new Headers object and copies headers from fetch arguments
 * @param fetchArgs Original fetch arguments containing input and options
 * @returns A new Headers object with copied headers from input and options
 */
function copyHeaders(fetchArgs: Parameters<Fetch>): Headers {
  const [input, options = {}] = fetchArgs;

  // Start with base headers from input if it's a Request, otherwise empty Headers
  const headers = new Headers(
    input instanceof Request ? input.headers : undefined,
  );

  // Copy any headers from options, overriding existing values
  if (options.headers) {
    Object.entries(options.headers).forEach(([key, value]) => {
      headers.set(key, value);
    });
  }

  return headers;
}

function buildTokenAuthConfig(token: string): TokenAuthConfig {
  return { token };
}

function buildPasswordAuthConfig(config: {
  user: string;
  password: string;
}): PasswordAuthConfig {
  return config;
}

interface IApiClientBase {
  // Fetch function for the Materialize HTTP API
  mzApiFetch: Fetch;
  // Auth configuration used for the Websocket API.
  // When null, we never attach the auth config. This occurs for Impersonation and Self Managed when using password auth.
  getWsAuthConfig: () => MaterializeAuthConfig | null;

  // The scheme used for the Materialize HTTP API.
  mzHttpUrlScheme: HttpScheme;
  // The scheme used for the Materialize Websocket API.
  mzWebsocketUrlScheme: WebsocketScheme;
}

interface ICloudApiClient {
  type: CloudAppMode;
  isImpersonating: boolean;
  // Fetch function for the Cloud API.
  cloudApiFetch: Fetch;
  // The base path for the Cloud Global API.
  cloudGlobalApiBasePath: string;
}

interface IFronteggApiClient {
  isImpersonating: false;
  // The base path for the Frontegg API.
  fronteggApiBasePath: string;
}

interface IImpersonationApiClient {
  isImpersonating: true;
  isLocalImpersonation: boolean;
}

interface ISelfManagedApiClient {
  type: SelfManagedAppMode;
  // The base path for the Password Auth Materialize HTTP API.
  authApiBasePath: string;
  authMode: SelfManagedAuthMode;
}

export class SelfManagedApiClient
  implements IApiClientBase, ISelfManagedApiClient
{
  #appConfig: Readonly<SelfManagedAppConfig>;
  authMode: SelfManagedAuthMode;
  authApiBasePath: string;
  mzHttpUrlScheme: HttpScheme;
  mzWebsocketUrlScheme: WebsocketScheme;
  mzApiFetch: Fetch;
  getWsAuthConfig: () => MaterializeAuthConfig | null;
  type = "self-managed" as const;

  #mzApiWithAuthRedirect = async (...req: Parameters<Fetch>) => {
    const response = await globalFetch(...req);
    // If the user is not authenticated, we redirect to the login page.
    if (response.status === 401) {
      await logoutAndRedirect({ apiClient: this });
    }
    return response;
  };

  constructor({ appConfig }: { appConfig: Readonly<SelfManagedAppConfig> }) {
    this.#appConfig = appConfig;
    this.mzHttpUrlScheme = this.#appConfig.environmentdScheme;
    this.mzWebsocketUrlScheme = this.#appConfig.environmentdWebsocketScheme;
    this.authApiBasePath = `${this.#appConfig.environmentdScheme}://${this.#appConfig.environmentdConfig.environmentdHttpAddress}`;
    this.authMode = this.#appConfig.authMode;

    this.mzApiFetch =
      this.authMode === "None" ? globalFetch : this.#mzApiWithAuthRedirect;

    this.getWsAuthConfig = () => {
      // Unintuitively, we return an auth config when authMode is "None". This is because
      // the authenticated websocket API gets the necessary information via the http-only cookie
      // and errors if you try to send a websocket message with the auth config.
      if (this.authMode === "None") {
        return buildPasswordAuthConfig(FLEXIBLE_DEPLOYMENT_USER);
      }
      return null;
    };
  }
}

export class ImpersonationApiClient
  implements IApiClientBase, ICloudApiClient, IImpersonationApiClient
{
  #appConfig: Readonly<CloudAppConfig>;
  cloudGlobalApiBasePath: string;
  type = "cloud" as const;
  isImpersonating = true as const;
  isLocalImpersonation: boolean;
  mzHttpUrlScheme: HttpScheme;
  mzWebsocketUrlScheme: WebsocketScheme;

  constructor({ appConfig }: { appConfig: Readonly<CloudAppConfig> }) {
    this.#appConfig = appConfig;
    this.cloudGlobalApiBasePath = this.#appConfig.cloudGlobalApiUrl;
    this.mzHttpUrlScheme = this.#appConfig.environmentdScheme;
    this.mzWebsocketUrlScheme = this.#appConfig.environmentdWebsocketScheme;
    this.isLocalImpersonation = this.#appConfig.isLocalImpersonation;
  }

  #impersonationCloudApiMiddleware: Middleware = (next) => {
    return async (...fetchArgs) => {
      const [input, options = {}] = fetchArgs;
      const headers = copyHeaders(fetchArgs);

      if (this.#appConfig.impersonation?.organizationId) {
        headers.set("accept", this.#appConfig.impersonation.organizationId);
        const request = new Request(input, {
          headers,
          credentials: "include" as const,
          method: "GET",
        });
        return next(request);
      }

      const request = new Request(input, { ...options, headers });
      return next(request);
    };
  };

  mzApiFetch = globalFetch;
  cloudApiFetch = withMiddleware(
    globalFetch,
    this.#impersonationCloudApiMiddleware,
  );
  getWsAuthConfig = () => null;
}

export class CloudApiClient
  implements IApiClientBase, ICloudApiClient, IFronteggApiClient
{
  #appConfig: Readonly<CloudAppConfig>;
  cloudGlobalApiBasePath: string;
  fronteggApiBasePath: string;
  type = "cloud" as const;
  isImpersonating = false as const;
  mzHttpUrlScheme: HttpScheme;
  mzWebsocketUrlScheme: WebsocketScheme;

  constructor({ appConfig }: { appConfig: Readonly<CloudAppConfig> }) {
    this.#appConfig = appConfig;
    this.cloudGlobalApiBasePath = this.#appConfig.cloudGlobalApiUrl;
    this.fronteggApiBasePath = this.#appConfig.fronteggUrl;
    this.mzHttpUrlScheme = this.#appConfig.environmentdScheme;
    this.mzWebsocketUrlScheme = this.#appConfig.environmentdWebsocketScheme;
  }

  #getAccessToken() {
    // Get the access token from Frontegg's context holder
    const accessToken = ContextHolder.for("default").getAccessToken();

    // The token will most likely always exist since this function
    // will be called when logged in. If not, we should alert Sentry.
    if (!accessToken) {
      Sentry.addBreadcrumb({
        level: "error",
        category: "auth",
        message: "Failed to refresh auth token",
      });
    }
    return accessToken;
  }

  #authMiddleware: Middleware = (next) => {
    return async (...fetchArgs) => {
      const [input, options = {}] = fetchArgs;
      const accessToken = this.#getAccessToken();

      const headers = copyHeaders(fetchArgs);
      if (accessToken) {
        headers.set("Authorization", `Bearer ${accessToken}`);
        if (this.#appConfig.sentryConfig?.release) {
          Object.entries(
            buildConsoleVersionHeaders(this.#appConfig.sentryConfig?.release),
          ).forEach(([key, value]) => {
            headers.set(key, value);
          });
        }
      }

      const request = new Request(input, { ...options, headers });
      return next(request);
    };
  };

  mzApiFetch = withMiddleware(globalFetch, this.#authMiddleware);
  cloudApiFetch = this.mzApiFetch;

  getWsAuthConfig = () => buildTokenAuthConfig(this.#getAccessToken() ?? "");
}

function createApiClient(appConfig: AppConfig) {
  if (appConfig.mode === "cloud") {
    if (appConfig.isImpersonating) {
      return new ImpersonationApiClient({ appConfig });
    }
    return new CloudApiClient({ appConfig });
  } else {
    return new SelfManagedApiClient({ appConfig });
  }
}

export const apiClient = createApiClient(appConfigSingleton);
