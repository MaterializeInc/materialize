// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { trackSignUpInHubspotActions } from "~/analytics/hubspot";
import {
  type FronteggProviderProps,
  type ISignUpCompletePayload,
  type IUserVerifiedPayload,
} from "~/external-library-wrappers/frontegg";
import { forgetExternalRedirect } from "~/utils/externalRedirect";

/**
 * Frontegg auth events. New users go to the Console, not back to a marketing
 * page saved by an earlier "Sign in" click: Frontegg fires these right before
 * it reads the saved redirect (signUpComplete for social sign-ups, userVerified
 * from the activation link for email sign-ups).
 */
export const buildFronteggEvents = (
  allowedRedirectOrigins: readonly string[],
): FronteggProviderProps["events"] => ({
  signUpComplete: (payload: ISignUpCompletePayload) => {
    forgetExternalRedirect(allowedRedirectOrigins);
    trackSignUpInHubspotActions.signUpComplete(payload);
  },
  userVerified: (payload: IUserVerifiedPayload) => {
    if (String(payload.origin) === "ACTIVATION_LINK") {
      forgetExternalRedirect(allowedRedirectOrigins);
    }
    trackSignUpInHubspotActions.userVerified(payload);
  },
});
