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
import { isSafeRedirectUrl } from "~/utils/redirectUrl";

/**
 * Sanitizes the redirectUrl query parameter on the login page to prevent open redirect attacks.
 *
 * This component checks if the current page is the login page and if the redirectUrl
 * query parameter contains a potentially malicious external URL. If so, it removes
 * the redirectUrl parameter to prevent the redirect.
 *
 * Examples of malicious URLs that are blocked:
 * - //example.com (protocol-relative URLs)
 * - https://example.com (absolute URLs)
 * - javascript:alert(1) (javascript URLs)
 */
export const RedirectUrlSanitizer = ({ children }: React.PropsWithChildren) => {
  const { pathname } = useLocation();
  const [searchParams] = useSearchParams();

  const isLoginPage = pathname === AUTH_ROUTES.loginPath;
  const redirectUrl = searchParams.get("redirectUrl");
  const needsSanitization =
    isLoginPage && redirectUrl && !isSafeRedirectUrl(redirectUrl);

  if (needsSanitization) {
    return <Navigate to={AUTH_ROUTES.loginPath} replace />;
  }

  return children;
};
