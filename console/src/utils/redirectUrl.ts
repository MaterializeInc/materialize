// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * Validates that a redirect URL is safe (internal only, no external redirects).
 *
 * This prevents open redirect vulnerabilities where attackers craft URLs like:
 * - //example.com (protocol-relative URL)
 * - https://example.com (absolute URL)
 * - javascript:alert(1) (javascript URLs)
 * - data:text/html,... (data URLs)
 * - /\t/evil.com (WHATWG URL parsers strip tabs/newlines, becoming //evil.com)
 *
 * @param url - The URL to validate
 * @returns true if the URL is a safe internal path, false otherwise
 */
export function isSafeRedirectUrl(url: string): boolean {
  // Empty or whitespace-only URLs are not safe (force default redirect)
  if (!url || !url.trim()) {
    return false;
  }

  const trimmedUrl = url.trim();

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
