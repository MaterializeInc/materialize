// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { isAllowedExternalRedirectUrl } from "~/utils/redirectUrl";
import storageAvailable from "~/utils/storageAvailable";

// Frontegg's own localStorage key (FRONTEGG_AFTER_AUTH_REDIRECT_URL in
// @frontegg/redux-store). Frontegg reads it after sign-in when the URL no longer
// has a `redirectUrl` param, then clears it.
export const FRONTEGG_AFTER_AUTH_REDIRECT_URL =
  "FRONTEGG_AFTER_AUTH_REDIRECT_URL";

/**
 * Social and SSO sign-in leave the Console for the identity provider and come
 * back on a callback URL without our `redirectUrl` param. Stash an allowed
 * external redirect where Frontegg looks for it so those flows return to the
 * marketing site too.
 */
export const rememberExternalRedirect = (redirectUrl: string) => {
  if (!storageAvailable("localStorage")) return;
  window.localStorage.setItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL, redirectUrl);
};

/**
 * A visitor who starts signing in from the marketing site and gives up would
 * otherwise be sent there the next time they sign in to the Console directly
 * (which arrives as `/account/login?redirectUrl=/`, or another internal path),
 * and a brand-new user would land on the marketing site instead of the
 * Console. Only our external redirects are cleared; Frontegg's own internal
 * paths stay.
 */
export const forgetExternalRedirect = (allowedOrigins: readonly string[]) => {
  if (!storageAvailable("localStorage")) return;
  const stored = window.localStorage.getItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL);
  if (stored && isAllowedExternalRedirectUrl(stored, allowedOrigins)) {
    window.localStorage.removeItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL);
  }
};
