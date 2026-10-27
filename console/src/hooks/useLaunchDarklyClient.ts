// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { User } from "@sentry/react";
import { initialize, LDClient, LDContext } from "launchdarkly-js-client-sdk";

// We use module state here because we need to ensure the client is only initialized
// once. Because of suspense and concurrent rendering, multiple renders may happen with
// the ref value uninitialized.
let ldClient: LDClient | null = null;
let ldClientReadyResolve: () => void | null;
let ldClientReady: Promise<void> | null = new Promise((resolve) => {
  ldClientReadyResolve = resolve;
});

export function buildLdContextForImpersonation(
  impersonationOrganizationId: string,
): LDContext {
  return {
    kind: "multi",
    impersonated_organization: {
      key: impersonationOrganizationId,
    },
  };
}

export function buildLdContext(user: User): LDContext {
  return {
    kind: "multi",
    ...(user
      ? {
          user: {
            key: user.id,
            email: user.email,
          },
        }
      : {}),
    ...(user.tenantId
      ? {
          organization: {
            key: user.tenantId,
          },
        }
      : {}),
  };
}

export function useLaunchDarklyClient(
  launchDarklyKey: string,
  ldContext: LDContext,
) {
  if (ldClient) return ldClient;

  ldClient = initialize(launchDarklyKey, ldContext, {
    bootstrap: "localStorage",
    sendEventsOnlyForVariation: true,
  });
  ldClient.on("ready", () => {
    ldClientReadyResolve();
    ldClientReady = null;
  });

  ldClient.on("error", (error: unknown) => {
    const cause = error instanceof Error ? error.message : "Unknown error";
    Sentry.captureException(
      new Error("LaunchDarkly failed to initialize", {
        cause,
      }),
    );
  });

  if (ldClientReady) {
    // Trigger suspense if the client isn't ready yet
    throw ldClientReady;
  }
  return ldClient;
}
