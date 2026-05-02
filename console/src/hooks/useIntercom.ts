// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import Intercom from "@intercom/messenger-js-sdk";
import { useEffect } from "react";

import { useIntercomJwt } from "~/api/auth";
const APP_ID = "r8661p0d";

export function useIntercomAnonymous() {
  useEffect(() => {
    const anonymousConfig = {
      app_id: APP_ID,
    };
    Intercom(anonymousConfig);
  }, []);
}

export function useIntercom() {
  const { intercomJwt } = useIntercomJwt();
  const jwt = intercomJwt?.jwt;

  useEffect(() => {
    if (jwt) {
      Intercom({ app_id: APP_ID, intercom_user_jwt: jwt });
    }
  }, [jwt]);
}

export default useIntercom;
