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
  forgetExternalRedirect,
  rememberExternalRedirect,
} from "~/utils/externalRedirect";
import {
  isAllowedExternalRedirectUrl,
  isSafeRedirectUrl,
} from "~/utils/redirectUrl";

export interface RedirectUrlSanitizerProps {
  /** External origins (the marketing site) that `redirectUrl` may point at. */
  allowedOrigins: readonly string[];
  children?: React.ReactNode;
}

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
  allowedOrigins,
  children,
}: RedirectUrlSanitizerProps) => {
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
      : undefined;

  React.useEffect(() => {
    if (!isLoginPage) return;
    if (externalRedirectUrl) {
      rememberExternalRedirect(externalRedirectUrl);
    } else {
      forgetExternalRedirect(allowedOrigins);
    }
  }, [isLoginPage, redirectUrl, externalRedirectUrl, allowedOrigins]);

  if (needsSanitization) {
    return <Navigate to={AUTH_ROUTES.loginPath} replace />;
  }

  return children;
};
