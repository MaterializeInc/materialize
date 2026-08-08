// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useEffect } from "react";

import { useIntercomJwt } from "~/api/auth";
const APP_ID = "r8661p0d";

/**
 * Loads the Intercom SDK and boots it off the critical path. The SDK module is
 * dynamically imported (own chunk) and the boot call is scheduled in idle time
 * so it does not compete with first-paint bandwidth/CPU.
 */
function bootIntercomDeferred(config: {
  app_id: string;
  intercom_user_jwt?: string;
}) {
  const schedule =
    typeof window.requestIdleCallback === "function"
      ? (cb: () => void) =>
          window.requestIdleCallback(cb, { timeout: 3000 })
      : (cb: () => void) => window.setTimeout(cb, 1000);
  schedule(async () => {
    const { default: Intercom } = await import("@intercom/messenger-js-sdk");
    Intercom(config);
  });
}

export function useIntercomAnonymous() {
  useEffect(() => {
    bootIntercomDeferred({ app_id: APP_ID });
  }, []);
}

export function useIntercom() {
  const { intercomJwt } = useIntercomJwt();
  const jwt = intercomJwt?.jwt;

  useEffect(() => {
    if (jwt) {
      bootIntercomDeferred({ app_id: APP_ID, intercom_user_jwt: jwt });
    }
  }, [jwt]);
}

export default useIntercom;
