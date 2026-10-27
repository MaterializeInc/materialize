// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/* eslint-disable no-restricted-imports */
/**
 * This file is a facade for the react-oidc-context / oidc-client-ts libraries.
 */
export {
  type AuthContextProps,
  AuthProvider,
  hasAuthParams,
  // We should not use this hook directly because it requires AuthProvider to be mounted,
  // which it only is when OIDC is available. Instead, use AppConfigSwitch.
  useAuth,
} from "react-oidc-context";

import { useQuery } from "@tanstack/react-query";
import { User, UserManager, WebStorageStateStore } from "oidc-client-ts";

import { apiClient } from "~/api/apiClient";
import { useAppConfig } from "~/config/useAppConfig";

export interface OidcConfig {
  issuer: string;
  clientId: string;
  scopes: string;
}

interface ConsoleConfigResponse {
  oidc_issuer: string;
  console_oidc_client_id: string;
  console_oidc_scopes: string;
}

async function fetchOidcConfig(): Promise<OidcConfig> {
  const response = await fetch("/api/console/config");
  if (!response.ok) {
    throw new Error(`Failed to fetch OIDC config: ${response.status}`);
  }
  const data: ConsoleConfigResponse = await response.json();

  if (!data.console_oidc_client_id) {
    throw new Error(
      "To use SSO, OIDC client ID must be set. Configure the console_oidc_client_id system parameter: https://materialize.com/docs/self-managed-deployments/configuration-system-parameters/",
    );
  }
  if (
    !data.console_oidc_scopes ||
    !data.console_oidc_scopes.includes("openid")
  ) {
    throw new Error(
      "To use SSO, OIDC scopes must include at least 'openid'. Configure the console_oidc_scopes system parameter: https://materialize.com/docs/self-managed-deployments/configuration-system-parameters/",
    );
  }

  return {
    issuer: data.oidc_issuer,
    clientId: data.console_oidc_client_id,
    scopes: data.console_oidc_scopes,
  };
}

/**
 * Wraps oidc-client-ts UserManager, caching the ID token for synchronous
 * access by the API client middleware and WebSocket auth.
 *
 * We use the ID token (not the access token) because its audience is always
 * the OIDC client ID, which matches what environmentd validates for both
 * HTTP and pgwire connections.
 */
export class MzOidcUserManager {
  #userManager: UserManager;
  #cachedToken: { value: string; expiresAtMs: number } | undefined;

  constructor(config: OidcConfig) {
    this.#userManager = new UserManager({
      authority: config.issuer,
      client_id: config.clientId,
      redirect_uri: `${window.location.origin}/auth/callback`,
      post_logout_redirect_uri: `${window.location.origin}/account/login`,
      scope: config.scopes,
      response_type: "code",
      automaticSilentRenew: true,
      userStore: new WebStorageStateStore({ store: window.localStorage }),
    });

    this.#userManager.events.addUserLoaded((user) => {
      this.#setCachedToken(user);
    });

    this.#userManager.events.addUserUnloaded(() => {
      this.#cachedToken = undefined;
    });

    // Eagerly populate the cached token from storage so that API requests
    // made immediately after page load can include the Authorization header.
    // The userLoaded event only fires on sign-in/silent-renew, not on
    // loading an existing session from storage.
    this.#userManager.getUser().then((user) => {
      if (user && !this.#cachedToken) {
        this.#setCachedToken(user);
      }
    });
  }

  // Don't send an expired id_token: the OIDC Bearer header makes environmentd
  // validate it instead of the password session cookie, breaking password
  // fallback.
  getIdToken(): string | undefined {
    if (!this.#cachedToken || this.#cachedToken.expiresAtMs <= Date.now()) {
      return undefined;
    }
    return this.#cachedToken.value;
  }

  // Extract the id_token and its `exp` (the claim environmentd validates) up
  // front, so getIdToken() can drop an expired token without decoding the JWT.
  #setCachedToken(user: User) {
    this.#cachedToken = user.id_token
      ? { value: user.id_token, expiresAtMs: user.profile.exp * 1000 }
      : undefined;
  }

  getUserManager(): UserManager {
    return this.#userManager;
  }

  signoutRedirect(): Promise<void> {
    return this.#userManager.signoutRedirect();
  }

  /**
   * Async factory that fetches OIDC config from environmentd's
   * `/api/console/config` endpoint and returns an initialized manager.
   */
  static async create(): Promise<MzOidcUserManager> {
    const config = await fetchOidcConfig();
    return new MzOidcUserManager(config);
  }
}

/**
 * Resolves the OIDC manager once initialization completes. Returns `null`
 * when not in OIDC mode or when init fails — callers should treat the
 * absence of a manager as "OIDC unavailable, fall back to password sign-in"
 */
export const useOidcManagerQuery = () => {
  const appConfig = useAppConfig();
  const isOidc =
    appConfig.mode === "self-managed" && appConfig.authMode === "Oidc";

  return useQuery({
    queryKey: ["oidc-manager"],
    queryFn: () => {
      if (
        apiClient.type !== "self-managed" ||
        !apiClient.oidcManagerInitializationPromise
      ) {
        return null;
      }
      return apiClient.oidcManagerInitializationPromise;
    },
    enabled: isOidc,
    staleTime: Infinity,
    retry: false,
    retryOnMount: false, // Do not retry on mount, otherwise we will retry indefinitely
  });
};
