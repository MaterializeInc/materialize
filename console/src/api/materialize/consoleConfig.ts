// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * The system parameters environmentd publishes so the console can sign users
 * in. Field names mirror the parameters themselves.
 */
interface ConsoleConfigResponse {
  oidc_issuer?: string;
  console_oidc_client_id?: string;
  console_oidc_scopes?: string;
  console_ory_sdk_url?: string;
}

export interface ConsoleConfig {
  issuer: string;
  oidcClientId: string;
  oidcScopes: string;
  /**
   * Where Ory's APIs are served, when the deployment runs Ory and lets the
   * console host its login flow. Empty otherwise: the console is then a plain
   * OIDC client and the issuer serves its own login pages.
   */
  oryUrl: string;
}

let configPromise: Promise<ConsoleConfig> | undefined;

/**
 * Configuration environmentd holds and the console cannot derive. Memoized: the
 * OIDC client and any Ory flow both need it on the same page load, and it does
 * not change while the tab is open.
 *
 * A rejection is not cached. The endpoint is unavailable whenever environmentd
 * is still starting, and remembering that failure would strand the console on
 * an error until the page is reloaded.
 */
export function fetchConsoleConfig(): Promise<ConsoleConfig> {
  if (!configPromise) {
    configPromise = requestConsoleConfig().catch((error) => {
      configPromise = undefined;
      throw error;
    });
  }
  return configPromise;
}

async function requestConsoleConfig(): Promise<ConsoleConfig> {
  const response = await fetch("/api/console/config");
  if (!response.ok) {
    throw new Error(
      "Could not read this environment's sign-in configuration. It may still be starting up.",
    );
  }
  const data: ConsoleConfigResponse = await response.json();
  return {
    issuer: data.oidc_issuer ?? "",
    oidcClientId: data.console_oidc_client_id ?? "",
    oidcScopes: data.console_oidc_scopes ?? "",
    oryUrl: data.console_ory_sdk_url ?? "",
  };
}

/** Discards the memoized config. Exported for tests. */
export function resetConsoleConfig() {
  configPromise = undefined;
}
