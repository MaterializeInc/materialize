// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { BuildConstants } from "./buildConstants";

export const getCloudGlobalApiUrl = ({
  stack,
  isImpersonation,
}: {
  stack: string;
  isImpersonation: boolean;
}) => {
  if (stack === "local") {
    // When pointing at a local kind cluster, the sync-server lives on port 32002
    return "http://127.0.0.1:32002";
  }
  if (isImpersonation) {
    if (window.location.hash === "#staging") {
      console.warn("Detected request for staging APIs");
      stack = "staging";
    }
    return `https://global-api-aws-us-east-1-${stack.replaceAll(
      ".",
      "-",
    )}.materialize.teleport.sh`;
  }
  if (stack === "production") {
    return "https://api.cloud.materialize.com";
  }
  return `https://api.${stack}.cloud.materialize.com`;
};

export const getFronteggUrl = (stack: string) => {
  if (stack === "production") {
    return `https://admin.cloud.materialize.com`;
  }
  if (stack === "local") {
    // local development against cloud services uses staging frontegg
    return `https://admin.staging.cloud.materialize.com`;
  }
  return `https://admin.${stack}.cloud.materialize.com`;
};

/**
 * Returns true if we should use TLS when talking to our own API endpoints.
 */
function isSqlApiTlsEnabled({
  buildConstants,
  isLocalImpersonation,
}: {
  buildConstants: BuildConstants;
  isLocalImpersonation: boolean;
}) {
  // SQL testing with mzcompose uses http
  if (process.env.NODE_ENV === "test" || isLocalImpersonation) {
    return false;
  }

  // For self managed, we figure this out by dynamically looking at the current URL's protocol.
  if (buildConstants.consoleDeploymentMode === "flexible-deployment") {
    return location.protocol === "https:";
  }
  // Local development via staging and production use endpoints that support TLS
  return true;
}

export function getEnvironmentdScheme({
  buildConstants,
  isLocalImpersonation,
}: {
  buildConstants: BuildConstants;
  isLocalImpersonation: boolean;
}) {
  return isSqlApiTlsEnabled({ buildConstants, isLocalImpersonation })
    ? ("https" as const)
    : ("http" as const);
}

export function getEnvironmentdWebsocketScheme({
  buildConstants,
  isLocalImpersonation,
}: {
  buildConstants: BuildConstants;
  isLocalImpersonation: boolean;
}) {
  return isSqlApiTlsEnabled({ buildConstants, isLocalImpersonation })
    ? ("wss" as const)
    : ("ws" as const);
}
