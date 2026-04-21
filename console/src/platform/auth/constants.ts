// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// `sessionStorage` key used to pass a one-shot message across the redirect
// that the login page reads and immediately clears.
export const LOGIN_REDIRECT_MESSAGE_KEY = "mz.loginRedirectMessage";

export type LoginReason = "auth_rejected" | "session_expired";

export type LoginRedirectMessage = {
  reason: LoginReason;
  detail?: string;
};

const AUTH_REJECTED_SUFFIX =
  " Please try again, or contact your administrator if this persists.";

export const LOGIN_REASON_MESSAGES: Record<LoginReason, string> = {
  auth_rejected: `Sign-in was rejected.${AUTH_REJECTED_SUFFIX}`,
  session_expired: "Your previous session ended. Please sign in again.",
};

/** Embeds the sanitized server-side detail (e.g. "invalid audience") into the
 * rejected-login copy. */
export const buildAuthRejectedMessage = (detail: string | null): string => {
  if (!detail) return LOGIN_REASON_MESSAGES.auth_rejected;
  return `Sign-in was rejected: ${detail}.${AUTH_REJECTED_SUFFIX}`;
};
