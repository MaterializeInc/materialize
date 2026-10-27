// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { assert, base32UuidDecode } from "~/util";

import { BuildConstants } from "./buildConstants";

const getOrganizationIdFromBase32 = (encoded: string) => {
  try {
    return base32UuidDecode(encoded.toUpperCase());
  } catch (e) {
    console.error(e);
    throw new Error("Invalid impersonation subdomain");
  }
};

export const getImpersonatedEnvironment = (
  consoleHost: string,
  buildConstants: BuildConstants,
) => {
  const host = buildConstants.impersonationHostname ?? consoleHost;
  const matches = host.match(/(^.*)\.materialize.teleport.sh/);
  if (matches && matches.length > 1) {
    const [, enviromentSubdomain] = matches;
    const [cloudProvider, ...parts] = enviromentSubdomain.split("-");
    const ordinal = parts.pop();
    const encodedOrganizationId = parts.pop();
    assert(encodedOrganizationId);
    const organizationId = getOrganizationIdFromBase32(
      encodedOrganizationId.toUpperCase(),
    );
    const cloudRegion = parts.join("-");
    return {
      provider: cloudProvider,
      region: cloudRegion,
      regionId: `${cloudProvider}/${cloudRegion}`,
      organizationId,
      environmentId: `environment-${organizationId}-${ordinal}`,
      // Since environmentd is reverse proxying console, it's the same hostname
      environmentdHttpAddress: consoleHost,
    };
  }
  return null;
};

export type ImpersonationConfig = ReturnType<typeof getImpersonatedEnvironment>;
