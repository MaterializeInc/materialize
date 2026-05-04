// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/** Reads the sanitized `OidcError::Display` body environmentd returns for OIDC
 * 401s. Returns undefined for the generic `"unauthorized"` body so callers can
 * distinguish a server-supplied detail from a routine 401. */
export const readAuthErrorDetail = async (
  response: Response,
): Promise<string | undefined> => {
  try {
    const body = (await response.clone().text()).trim();
    if (!body || body === "unauthorized") return undefined;
    return body;
  } catch {
    return undefined;
  }
};
