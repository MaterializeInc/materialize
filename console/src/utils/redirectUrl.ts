// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildConstants } from "~/config/buildConstants";

/**
 * Origins outside the Console that sign-in may send the visitor back to. The
 * marketing site links to the login page with `?redirectUrl=<page>` so visitors
 * return to the page they signed in from.
 */
const MARKETING_SITE_ORIGINS = [
  "https://materialize.com",
  "https://www.materialize.com",
];

/**
 * Returns the external origins `redirectUrl` may point at for the given stack.
 *
 * `devOrigins` is a local marketing site's dev server. It only comes from
 * local `yarn start` builds (`__MARKETING_SITE_DEV_ORIGINS__`); production
 * builds, which staging and previews also serve, set it to none. Even then,
 * the production stack only allows the marketing site itself.
 */
export function getAllowedRedirectOrigins(
  currentStack: string,
  devOrigins: readonly string[] = buildConstants.marketingSiteDevOrigins,
): string[] {
  if (currentStack === "production") {
    return [...MARKETING_SITE_ORIGINS];
  }
  return [...MARKETING_SITE_ORIGINS, ...devOrigins];
}

/**
 * True when `url` is an absolute URL whose origin exactly matches one of
 * `allowedOrigins`. Origins are compared after parsing, so look-alikes such as
 * `https://materialize.com.evil.com` or `https://materialize.com@evil.com` fail.
 */
export function isAllowedExternalRedirectUrl(
  url: string,
  allowedOrigins: readonly string[],
): boolean {
  if (!url || allowedOrigins.length === 0) {
    return false;
  }
  const trimmedUrl = url.trim();
  // The browser strips these while parsing, so the raw string could disagree
  // with what we validated.
  if (/[\t\n\r\\]/.test(trimmedUrl)) {
    return false;
  }
  let parsed: URL;
  try {
    parsed = new URL(trimmedUrl);
  } catch {
    return false;
  }
  if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
    return false;
  }
  if (parsed.username || parsed.password) {
    return false;
  }
  return allowedOrigins.includes(parsed.origin);
}

/**
 * Validates that a redirect URL is safe: an internal path, or an absolute URL on
 * one of `allowedOrigins` (empty by default, so only internal paths pass).
 *
 * This prevents open redirect vulnerabilities where attackers craft URLs like:
 * - //example.com (protocol-relative URL)
 * - https://example.com (absolute URL)
 * - javascript:alert(1) (javascript URLs)
 * - data:text/html,... (data URLs)
 * - /\t/evil.com (WHATWG URL parsers strip tabs/newlines, becoming //evil.com)
 *
 * @param url - The URL to validate
 * @param allowedOrigins - External origins that are also allowed
 * @returns true if the URL is a safe internal path or an allowed external URL
 */
export function isSafeRedirectUrl(
  url: string,
  allowedOrigins: readonly string[] = [],
): boolean {
  // Empty or whitespace-only URLs are not safe (force default redirect)
  if (!url || !url.trim()) {
    return false;
  }

  const trimmedUrl = url.trim();

  if (isAllowedExternalRedirectUrl(trimmedUrl, allowedOrigins)) {
    return true;
  }

  // Protocol-relative URLs (//example.com) - NOT safe
  if (trimmedUrl.startsWith("//")) {
    return false;
  }

  // Absolute URLs with protocol - NOT safe
  // This catches http://, https://, javascript:, data:, etc.
  if (/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(trimmedUrl)) {
    return false;
  }

  // URLs with backslashes can be used to bypass checks (//example.com -> \/example.com)
  if (trimmedUrl.includes("\\")) {
    return false;
  }

  // URLs with tabs, newlines, or carriage returns can bypass // detection
  // Browser URL parsers strip these per WHATWG spec, turning /\t/evil.com into //evil.com
  if (/[\t\n\r]/.test(trimmedUrl)) {
    return false;
  }

  // Must start with a single forward slash (relative path)
  if (!trimmedUrl.startsWith("/")) {
    return false;
  }

  // Additional check: ensure the URL doesn't become protocol-relative after normalization
  // Some browsers normalize paths, so /\/example.com could become //example.com
  const normalizedPath = trimmedUrl.replace(/\/+/g, "/");
  if (normalizedPath !== "/" && normalizedPath.startsWith("//")) {
    return false;
  }

  return true;
}
