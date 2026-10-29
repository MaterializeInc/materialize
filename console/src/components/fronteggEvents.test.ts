// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { beforeEach, describe, expect, it } from "vitest";

import {
  type ISignUpCompletePayload,
  type IUserVerifiedPayload,
  UserVerifiedOriginTypes,
} from "~/external-library-wrappers/frontegg";
import { FRONTEGG_AFTER_AUTH_REDIRECT_URL } from "~/utils/externalRedirect";

import { buildFronteggEvents } from "./fronteggEvents";

const MARKETING_PAGE = "https://materialize.com/pricing/";

const buildEvents = () => buildFronteggEvents(["https://materialize.com"])!;

const verified = (origin: UserVerifiedOriginTypes): IUserVerifiedPayload => ({
  email: "ada@example.com",
  origin,
});

describe("buildFronteggEvents", () => {
  beforeEach(() => {
    localStorage.setItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL, MARKETING_PAGE);
  });

  it("sends new social sign-ups to the Console, not a saved marketing page", () => {
    buildEvents().signUpComplete!({
      email: "ada@example.com",
    } as ISignUpCompletePayload);

    expect(localStorage.getItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL)).toBeNull();
  });

  it("sends new email sign-ups (activation link) to the Console", () => {
    buildEvents().userVerified!(
      verified(UserVerifiedOriginTypes.ACTIVATION_LINK),
    );

    expect(localStorage.getItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL)).toBeNull();
  });

  it("keeps the saved marketing page for passwordless sign-ins", () => {
    buildEvents().userVerified!(verified(UserVerifiedOriginTypes.PASSWORDLESS));

    expect(localStorage.getItem(FRONTEGG_AFTER_AUTH_REDIRECT_URL)).toBe(
      MARKETING_PAGE,
    );
  });
});
