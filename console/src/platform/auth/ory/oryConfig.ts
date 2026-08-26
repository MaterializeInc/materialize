// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { fetchConsoleConfig } from "~/api/materialize/consoleConfig";
import { Configuration, FrontendApi } from "~/external-library-wrappers/ory";

/**
 * Ory has to be same-site with the console or the browser drops its CSRF and
 * session cookies, and the flow then fails partway through with nothing that
 * points at the cause. Warned about rather than rejected: a registrable domain
 * cannot be derived from a hostname without a public suffix list, so comparing
 * the last two labels misjudges a multi-part suffix such as `co.uk`.
 */
function warnIfCrossSite(sdkUrl: string) {
  if (!import.meta.env.DEV) return;
  const registrable = (host: string) => host.split(".").slice(-2).join(".");
  try {
    const oryHost = new URL(sdkUrl).hostname;
    if (registrable(oryHost) !== registrable(window.location.hostname)) {
      // eslint-disable-next-line no-console
      console.warn(
        `Ory is served from ${oryHost} but the console from ${window.location.hostname}. ` +
          "Cookies are scoped to a registrable domain, so sign-in will fail unless the two share one.",
      );
    }
  } catch {
    // Not a URL at all. The client construction below reports that itself.
  }
}

/**
 * Where Ory lives is the operator's choice, so only environmentd knows it. An
 * empty value means the deployment does not run Ory: the console is then a
 * plain OIDC client and the issuer serves its own login pages.
 */
async function resolveOrySdkUrl(): Promise<string> {
  // Development only. Self-managed ships one image to every deployment, so a
  // build-time value baked into the bundle would silently override whatever
  // each operator configured.
  if (import.meta.env.DEV && import.meta.env.VITE_ORY_SDK_URL) {
    return import.meta.env.VITE_ORY_SDK_URL;
  }

  const { oryUrl } = await fetchConsoleConfig();
  if (!oryUrl) {
    throw new Error(
      "This environment is not configured to sign in through Ory. Set the " +
        "console_ory_sdk_url system parameter.",
    );
  }
  warnIfCrossSite(oryUrl);
  return oryUrl;
}

let clientPromise: Promise<FrontendApi> | undefined;

/**
 * Shared Ory API client. Callers must not construct their own: a new instance
 * changes the identity of any callback closing over it, which re-triggers
 * flow-fetching effects in a loop.
 *
 * A rejection is not cached, so a console that loaded while environmentd was
 * still starting recovers on the next attempt rather than needing a reload.
 */
export function getOryClient(): Promise<FrontendApi> {
  if (!clientPromise) {
    clientPromise = resolveOrySdkUrl()
      .then(
        (url) =>
          new FrontendApi(
            new Configuration({
              basePath: url,
              credentials: "include",
              // Without an explicit JSON accept, Kratos treats a self-service
              // flow request as a page load and 303s to its own hosted UI,
              // which a cross-origin fetch cannot follow.
              headers: { Accept: "application/json" },
            }),
          ),
      )
      .catch((error) => {
        clientPromise = undefined;
        throw error;
      });
  }
  return clientPromise;
}

/** Discards the memoized client. Exported for tests. */
export function resetOryClient() {
  clientPromise = undefined;
}
