// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import React from "react";

import { dummyLoginFlow } from "~/external-library-wrappers/__mocks__/ory";
import { createBrowserLoginFlow } from "~/external-library-wrappers/__mocks__/ory";
import {
  type MzOidcUserManager,
  useOidcManagerQuery,
} from "~/external-library-wrappers/oidc";
import { renderComponent } from "~/test/utils";

import { LoginRoute } from "./UnauthenticatedRoutes";

/** A live OIDC token is the cheapest way to look signed in: it skips the
 * server probe the route otherwise makes for a password session cookie. */
const signedIn = () =>
  vi.mocked(useOidcManagerQuery).mockReturnValue({
    data: { getIdToken: () => "a-token" } as unknown as MzOidcUserManager,
    isLoading: false,
  } as unknown as ReturnType<typeof useOidcManagerQuery>);

describe("LoginRoute", () => {
  beforeEach(() => {
    vi.stubEnv("VITE_ORY_SDK_URL", "http://localhost:4000");
    createBrowserLoginFlow.mockResolvedValue(dummyLoginFlow);
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.clearAllMocks();
  });

  // Hydra sends a signed-in user back here when an OAuth2 client asks for
  // re-authentication. Redirecting them into the app strands that request with
  // no way to finish, so the flow has to win over the signed-in shortcut.
  it("renders the Ory flow for a challenge even when already signed in", async () => {
    signedIn();

    await renderComponent(<LoginRoute />, {
      initialRouterEntries: ["/account/login?login_challenge=challenge-abc"],
    });

    // A regex, not the exact name: a single-provider flow submits itself, and
    // Chakra's spinner adds a visually hidden "Loading..." to the button's name.
    expect(
      await screen.findByRole("button", { name: /Sign in with Auth0/ }),
    ).toBeInTheDocument();
  });

  it("sends a signed-in user to the app when no flow is in progress", async () => {
    signedIn();

    await renderComponent(<LoginRoute />, {
      initialRouterEntries: ["/account/login"],
    });

    expect(createBrowserLoginFlow).not.toHaveBeenCalled();
    expect(
      screen.queryByRole("button", { name: /Sign in with Auth0/ }),
    ).not.toBeInTheDocument();
  });
});
