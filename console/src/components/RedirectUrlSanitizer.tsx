// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { Navigate, useLocation, useSearchParams } from "react-router-dom";

import { AUTH_ROUTES } from "~/fronteggRoutes";
import {
  isAllowedExternalRedirectUrl,
  isSafeRedirectUrl,
} from "~/utils/redirectUrl";
import storageAvailable from "~/utils/storageAvailable";

// Frontegg's own localStorage key (FRONTEGG_AFTER_AUTH_REDIRECT_URL in
// @frontegg/redux-store). Frontegg reads it after sign-in when the URL no longer
// has a `redirectUrl` param, then clears it.
const FRONTEGG_AFTER_AUTH_REDIRECT_URL = "FRONTEGG_AFTER_AUTH_REDIRECT_URL";

/**
 * Social and SSO sign-in leave the Console for the identity provider and come
 * back on a callback URL without our `redirectUrl` param. Stash an allowed
 * external redirect where Frontegg looks for it so those flows return to the
 * marketing site too.
 */
const rememberExternalRedirect = (redirectUrl: string) => {
  if (!storageAvailable("localStorage")) return;
  window.localStorage.setItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL, redirectUrl);
};

/**
 * A visitor who starts signing in from the marketing site and gives up would
 * otherwise be sent there the next time they sign in to the Console directly.
 * Only our external redirects are cleared; Frontegg's own internal paths stay.
 */
const forgetExternalRedirect = (allowedOrigins: readonly string[]) => {
  if (!storageAvailable("localStorage")) return;
  const stored = window.localStorage.getItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL);
  if (stored && isAllowedExternalRedirectUrl(stored, allowedOrigins)) {
    window.localStorage.removeItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL);
  }
};

/**
 * Sanitizes the redirectUrl query parameter on the login page to prevent open redirect attacks.
 *
 * This component checks if the current page is the login page and if the redirectUrl
 * query parameter contains a potentially malicious external URL. If so, it removes
 * the redirectUrl parameter to prevent the redirect. Absolute URLs on
 * `allowedOrigins` (the marketing site) are kept.
 *
 * Examples of malicious URLs that are blocked:
 * - //example.com (protocol-relative URLs)
 * - https://example.com (absolute URLs not in allowedOrigins)
 * - javascript:alert(1) (javascript URLs)
 */
export const RedirectUrlSanitizer = ({
  allowedOrigins = [],
  children,
}: React.PropsWithChildren<{ allowedOrigins?: readonly string[] }>) => {
  const { pathname } = useLocation();
  const [searchParams] = useSearchParams();

  const isLoginPage = pathname === AUTH_ROUTES.loginPath;
  const redirectUrl = searchParams.get("redirectUrl");
  const needsSanitization =
    isLoginPage &&
    redirectUrl &&
    !isSafeRedirectUrl(redirectUrl, allowedOrigins);
  const externalRedirectUrl =
    isLoginPage &&
    redirectUrl &&
    isAllowedExternalRedirectUrl(redirectUrl, allowedOrigins)
      ? redirectUrl.trim()
      : null;

  React.useEffect(() => {
    if (!isLoginPage) return;
    if (externalRedirectUrl) {
      rememberExternalRedirect(externalRedirectUrl);
    } else if (!redirectUrl) {
      forgetExternalRedirect(allowedOrigins);
    }
  }, [isLoginPage, redirectUrl, externalRedirectUrl, allowedOrigins]);

  if (needsSanitization) {
    return <Navigate to={AUTH_ROUTES.loginPath} replace />;
  }

  return children;
};
