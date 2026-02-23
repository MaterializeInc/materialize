// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import * as Sentry from "@sentry/react";
import { atom } from "jotai";

import { buildGlobalQueryKey } from "~/api/buildQueryKeySchema";
import {
  type ISignUpCompletePayload,
  type IUserVerifiedPayload,
} from "~/external-library-wrappers/frontegg";
import { getStore } from "~/jotai";
import { getQueryClient } from "~/queryClient";
import { notNullOrUndefined } from "~/util";

import {
  MaterializeUseCase,
  PrimaryDataSource,
  ProjectDescription,
  RoleDescription,
} from "./onboardingSurveyOptions";
import { segment } from "./segment";

async function getAnonymousId() {
  if (segment) {
    const user = await segment.user();
    return user.anonymousId();
  }
}

function getHubspotUtk() {
  return document.cookie.replace(
    // This is the "recommended" way to get the hubspotutk cookie value from the hubspot forums
    // more specifically https://www.stephanieogaygarcia.com/hubspot-website-development/get-hubspot-cookie-hubspotutk-using-javascript
    // but eslint doesn't like the regex, so we're disabling it for this line
    // eslint-disable-next-line no-useless-escape
    /(?:(?:^|.*;\s*)hubspotutk\s*\=\s*([^;]*).*$)|^.*$/,
    "$1",
  );
}

function buildFormSubmissionUrl({
  portalId,
  formGuid,
}: {
  portalId: string;
  formGuid: string;
}) {
  return `https://api.hsforms.com/submissions/v3/integration/submit/${portalId}/${formGuid}`;
}

export const TRACK_SIGNUP_API_ENDPOINT = buildFormSubmissionUrl({
  portalId: "23399445",
  formGuid: "e1f0065e-c5ec-4004-afb7-d4aeab96c6b2",
});

export const ONBOARDING_SURVEY_API_ENDPOINT = buildFormSubmissionUrl({
  portalId: "23399445",
  formGuid: "7038d05b-0ad2-4c85-8ef7-36a163c6ddaf",
});

type HubspotPayload = {
  fields: { name: string; value: string }[];
  context: { pageUri: string; hutk?: string };
};

export const hubspotQueryKeys = {
  all: () => buildGlobalQueryKey("hubspot"),
  trackSignup: () => [
    ...hubspotQueryKeys.all(),
    buildGlobalQueryKey("track-signup"),
  ],
  onboardingSurvey: () => [
    ...hubspotQueryKeys.all(),
    buildGlobalQueryKey("onboarding-survey"),
  ],
};

export async function trackSignupInHubspot(
  payload: {
    email: string;
    name?: string;
    companyName?: string;
    socialProvider?: string;
  },
  options?: { retryDelay?: number },
) {
  // HubSpot submissions for non-social logins are handled separately.
  if (payload.socialProvider === undefined) {
    return;
  }

  let first, last, rest;
  if (payload.name) {
    [first, ...rest] = payload.name.split(" ");
    last = rest.join(" ");
  } else {
    first = "Unknown";
    last = "Unknown";
  }

  const data: HubspotPayload = {
    fields: [
      { name: "email", value: payload.email },
      { name: "firstname", value: first },
      { name: "lastname", value: last },
      { name: "company", value: payload.companyName ?? "" },
    ],
    context: {
      pageUri: window.location.href,
    },
  };

  const anonymousId = await getAnonymousId();
  if (anonymousId) {
    data.fields.push({ name: "segment_anonymous_id", value: anonymousId });
  }

  const hutk = getHubspotUtk();
  if (hutk) {
    data.context = {
      ...data.context,
      hutk,
    };
  }

  return getQueryClient()
    .getMutationCache()
    .build(getQueryClient(), {
      mutationKey: hubspotQueryKeys.trackSignup(),
      mutationFn: (params: HubspotPayload) => {
        return fetch(TRACK_SIGNUP_API_ENDPOINT, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(params),
        });
      },
      retry: 3,
      retryDelay: options?.retryDelay,
      onError: (error) => {
        Sentry.captureException(
          new Error("Hubspot signup tracking failed", { cause: error }),
        );
      },
    })
    .execute(data);
}

/**
 * Manages the state of Frontegg's signup to send to Hubspot.
 *
 * We need to globally store the payload state because Frontegg sends part of the
 * payload in signupComplete while the rest in userVerified. Frontegg calls
 * signupComplete first, then userVerified.
 */
export const trackSignUpInHubspotStateAtom = atom<
  | {
      state: "initial";
    }
  | {
      state: "signedUp";
      email: string;
      companyName?: string;
      socialProvider?: string;
    }
>({
  state: "initial",
});

export const trackSignUpInHubspotActions = {
  signUpComplete: (payload: ISignUpCompletePayload) => {
    const store = getStore();

    store.set(trackSignUpInHubspotStateAtom, {
      state: "signedUp",
      email: payload.email,
      companyName: payload.companyName,
      socialProvider: payload.socialProvider,
    });
  },
  userVerified: (payload: IUserVerifiedPayload) => {
    const store = getStore();
    const signUpState = store.get(trackSignUpInHubspotStateAtom);

    if (signUpState.state === "signedUp") {
      const { email, companyName, socialProvider } = signUpState;
      trackSignupInHubspot({
        email: email,
        companyName: companyName,
        socialProvider: socialProvider,
        name: payload.name,
      });
      store.set(trackSignUpInHubspotStateAtom, {
        state: "initial",
      });
    }
  },
};

type OnboardingSurveyPayload = {
  email: string;
  organizationId: string;
  userId: string;
  roleDescription: RoleDescription;
  roleDescriptionDetails?: string;
  materializeUseCase: MaterializeUseCase;
  materializeUseCaseDetails?: string;
  projectDescription: ProjectDescription;
  projectDescriptionDetails?: string;
  primaryDataSource: PrimaryDataSource;
  primaryDataSourceDetails?: string;
};

export async function submitOnboardingSurvey(
  payload: OnboardingSurveyPayload,
  options?: { retryDelay?: number },
) {
  const data: HubspotPayload = {
    fields: [
      { name: "email", value: payload.email },
      { name: "organization_id", value: payload.organizationId },
      { name: "mz_user_id", value: payload.userId },

      {
        name: "how_would_you_describe_your_role_",
        value: payload.roleDescription,
      },

      {
        name: "why_are_you_trying_materialize_",
        value: payload.materializeUseCase,
      },

      {
        name: "what_are_you_trying_to_solve_with_materialize_",
        value: payload.projectDescription,
      },

      {
        name: "what_is_the_primary_or_origin_data_source_",
        value: payload.primaryDataSource,
      },
    ],
    context: {
      pageUri: window.location.href,
    },
  };

  [
    {
      name: "how_would_you_describe_your_role__details",
      value: payload.roleDescriptionDetails,
    },
    {
      name: "why_are_you_trying_materialize__details",
      value: payload.materializeUseCaseDetails,
    },
    {
      name: "what_are_you_trying_to_solve_with_materialize__details",
      value: payload.projectDescriptionDetails,
    },
    {
      name: "what_is_the_primary_or_origin_data_source__details",
      value: payload.primaryDataSourceDetails,
    },
  ].forEach(({ name, value }) => {
    if (notNullOrUndefined(value)) {
      data.fields.push({
        name: name,
        value: value,
      });
    }
  });

  const hutk = getHubspotUtk();
  if (hutk) {
    data.context = {
      ...data.context,
      hutk,
    };
  }

  return getQueryClient()
    .getMutationCache()
    .build(getQueryClient(), {
      mutationKey: hubspotQueryKeys.onboardingSurvey(),
      mutationFn: (params: HubspotPayload) => {
        return fetch(ONBOARDING_SURVEY_API_ENDPOINT, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(params),
        });
      },
      retry: 3,
      retryDelay: options?.retryDelay,
      onError: (error) => {
        Sentry.captureException(
          new Error("Hubspot onboarding survey submit failed", {
            cause: error,
          }),
        );
      },
    })
    .execute(data);
}
