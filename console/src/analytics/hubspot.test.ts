// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { http, HttpResponse } from "msw";

import server from "~/api/mocks/server";
import {
  type IUserVerifiedPayload,
  SocialLoginProviders,
} from "~/external-library-wrappers/frontegg";
import { getStore } from "~/jotai";

import {
  TRACK_SIGNUP_API_ENDPOINT,
  trackSignupInHubspot,
  trackSignUpInHubspotActions,
  trackSignUpInHubspotStateAtom,
} from "./hubspot";

const trackSignupSuccessHandlerSpy = vi.fn();

const trackSignupSuccessHandler = http.post(TRACK_SIGNUP_API_ENDPOINT, () => {
  trackSignupSuccessHandlerSpy();
  return HttpResponse.json({ ok: true });
});
const trackSignupErrorHandler = http.post(TRACK_SIGNUP_API_ENDPOINT, () => {
  return HttpResponse.error();
});

describe("trackSignupInHubspot", () => {
  beforeEach(() => {
    vi.resetAllMocks();
  });

  it("successfully calls the hubspot api", async () => {
    server.use(trackSignupSuccessHandler);
    await trackSignupInHubspot({
      name: "Some User",
      companyName: "Some company",
      email: "someuser@example.com",
      socialProvider: SocialLoginProviders.Google,
    });
    expect(trackSignupSuccessHandlerSpy).toHaveBeenCalledOnce();
  });

  it("does not call if there is no social provider", async () => {
    server.use(trackSignupSuccessHandler);
    await trackSignupInHubspot({
      name: "Some User",
      companyName: "Some company",
      email: "someuser@example.com",
    });
    expect(trackSignupSuccessHandlerSpy).not.toHaveBeenCalled();
  });

  it("retries if the the call fails initially", async () => {
    server.use(
      http.post(
        TRACK_SIGNUP_API_ENDPOINT,
        () => {
          return HttpResponse.error();
        },
        { once: true },
      ),
      trackSignupSuccessHandler,
    );
    await trackSignupInHubspot(
      {
        name: "Some User",
        companyName: "Some company",
        email: "someuser@example.com",
        socialProvider: SocialLoginProviders.Google,
      },
      { retryDelay: 10 },
    );
    expect(trackSignupSuccessHandlerSpy).toHaveBeenCalledOnce();
  });

  it("fails after 3 tries", async () => {
    server.use(trackSignupErrorHandler);
    await expect(() =>
      trackSignupInHubspot(
        {
          name: "Some User",
          companyName: "Some company",
          email: "someuser@example.com",
          socialProvider: SocialLoginProviders.Google,
        },
        { retryDelay: 10 },
      ),
    ).rejects.toThrowError();
    expect(trackSignupSuccessHandlerSpy).not.toHaveBeenCalled();
  });
});

describe("trackSignUpInHubspotActions", () => {
  it("should reset state on userVerified when in signedUp state", async () => {
    const store = getStore();

    // Set initial signedUp state
    store.set(trackSignUpInHubspotStateAtom, {
      state: "signedUp",
      email: "test@example.com",
      companyName: "Test Co",
      socialProvider: "google",
    });

    trackSignUpInHubspotActions.userVerified({
      name: "Test User",
    } as IUserVerifiedPayload);

    // Verify state was reset
    expect(store.get(trackSignUpInHubspotStateAtom)).toEqual({
      state: "initial",
    });
  });

  it("should not leave the initial state when userVerified is called first", async () => {
    const store = getStore();

    trackSignUpInHubspotActions.userVerified({
      name: "Test User",
    } as IUserVerifiedPayload);

    expect(store.get(trackSignUpInHubspotStateAtom)).toEqual({
      state: "initial",
    });
  });
});
