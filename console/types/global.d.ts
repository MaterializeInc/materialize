// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

declare global {
  const __BASENAME__: string;
  const __CONSOLE_DEPLOYMENT_MODE__: "mz-cloud" | "flexible-deployment";
  const __DEFAULT_STACK__: string;
  const __FORCE_OVERRIDE_STACK__: string | undefined;
  const __MZ_CONSOLE_IMAGE_TAG__: string | undefined;
  const __SENTRY_ENABLED__: string;
  const __SENTRY_RELEASE__: string;
  const __IMPERSONATION_HOSTNAME__: string | undefined;

  interface Window {
    // Global object for hubspot tracking code
    _hsq: Array<["identify" | "trackPageView", (string | object)?]>;
  }
}

export {};
