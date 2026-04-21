// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { type LoginReason } from "~/platform/auth/constants";

// Cap on the forwarded OIDC detail so a malformed 401 body can't bloat the URL.
const MAX_AUTH_DETAIL_LENGTH = 200;

const hasBearer = (input: Parameters<typeof fetch>[0]): boolean =>
  input instanceof Request &&
  (input.headers.get("Authorization")?.startsWith("Bearer ") ?? false);

/** Reads the sanitized `OidcError::Display` environmentd returns for OIDC 401s. */
export const readAuthErrorDetail = async (
  response: Response,
): Promise<string | undefined> => {
  try {
    const body = (await response.clone().text()).trim();
    if (!body || body === "unauthorized") return undefined;
    return body.slice(0, MAX_AUTH_DETAIL_LENGTH);
  } catch {
    return undefined;
  }
};

// Returns `auth_rejected` only when a Bearer was sent.
export const formatLoginErrorForOIDC = (
  input: Parameters<typeof fetch>[0],
): LoginReason | undefined => (hasBearer(input) ? "auth_rejected" : undefined);
