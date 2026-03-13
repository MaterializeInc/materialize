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
 * It is used primarily to mock the libraries in tests via `vi.mock` in ~/vitest.setup.ts.
 * Make sure anything you'd like to mock is updated in ./__mocks__/oidc.ts
 */
export { UserManager } from "oidc-client-ts";
export { AuthProvider, hasAuthParams, useAuth } from "react-oidc-context";

import { UserManager } from "oidc-client-ts";

import { type OidcConfig } from "~/config/AppConfig";

/**
 * Shared OIDC UserManager instance. Created once by initOidcUserManager()
 * and used by both AuthProvider (React) and the API client (non-React).
 *
 * A cached ID token is maintained via UserManager events so that synchronous
 * callers (like getWsAuthConfig) can access it without awaiting.
 *
 * We use the ID token (not the access token) because its audience is always
 * the OIDC client ID, which matches what environmentd validates for both
 * HTTP and pgwire connections.
 */
let userManager: UserManager | null = null;
let cachedIdToken: string | undefined;

export function initOidcUserManager(config: OidcConfig): UserManager {
  if (userManager) return userManager;

  userManager = new UserManager({
    authority: config.issuer,
    client_id: config.clientId,
    redirect_uri: `${window.location.origin}/auth/callback`,
    post_logout_redirect_uri: `${window.location.origin}/account/login`,
    scope: config.scopes,
    response_type: "code",
    automaticSilentRenew: true,
  });

  userManager.events.addUserLoaded((user) => {
    cachedIdToken = user.id_token;
  });

  userManager.events.addUserUnloaded(() => {
    cachedIdToken = undefined;
  });

  // Eagerly populate the cached token from storage so that API requests
  // made immediately after page load can include the Authorization header.
  // The userLoaded event only fires on sign-in/silent-renew, not on
  // loading an existing session from storage.
  userManager.getUser().then((user) => {
    if (user && !cachedIdToken) {
      cachedIdToken = user.id_token;
    }
  });

  return userManager;
}

export function getOidcUserManager(): UserManager | null {
  return userManager;
}

export function getOidcIdToken(): string | undefined {
  return cachedIdToken;
}
