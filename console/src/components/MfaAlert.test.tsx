// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { subDays } from "date-fns";
import { http, HttpResponse } from "msw";
import React from "react";

import { buildFronteggUrl } from "~/api/frontegg";
import server from "~/api/mocks/server";
import { dummyValidUser } from "~/external-library-wrappers/__mocks__/frontegg";
import {
  useAuthUser,
  UserManagedByEnum,
} from "~/external-library-wrappers/frontegg";
import { renderComponent } from "~/test/utils";

import { MFA_DISMISSED_KEY, MfaAlert } from "./MfaAlert";

const dontForceMfaPolicyHandler = http.get(
  buildFronteggUrl("/frontegg/identity/resources/configurations/v1/mfa-policy"),
  () => {
    return HttpResponse.json({ enforceMFAType: "DontForce" });
  },
);
const forceMfaPolicyHandler = http.get(
  buildFronteggUrl("/frontegg/identity/resources/configurations/v1/mfa-policy"),
  () => {
    return HttpResponse.json({ enforceMFAType: "Force" });
  },
);

describe("MfaAlert", () => {
  beforeEach(() => {
    // Setup for showing MFA banner
    vi.mocked(useAuthUser).mockReturnValue({
      ...dummyValidUser,
      managedBy: UserManagedByEnum.FRONTEGG,
    });
    server.use(dontForceMfaPolicyHandler);
  });

  afterEach(() => {
    vi.resetAllMocks();
  });

  it("renders the MFA alert when conditions are met", async () => {
    renderComponent(<MfaAlert />);

    expect(await screen.findByTestId("mfa-required-alert")).toBeVisible();
    expect(screen.getByText(/Please for your organization./i)).toBeVisible();
    expect(screen.getByRole("link", { name: "require MFA" })).toBeVisible();
  });

  it("clicking the close button works", async () => {
    const user = userEvent.setup();
    renderComponent(<MfaAlert />);

    expect(await screen.findByTestId("mfa-required-alert")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "Close" }));
    expect(screen.queryByTestId("mfa-required-alert")).not.toBeInTheDocument();
  });

  it("renders the MFA alert after being dismissed for 30 days", async () => {
    localStorage.setItem(
      MFA_DISMISSED_KEY,
      JSON.stringify(subDays(new Date(), 30)),
    );
    renderComponent(<MfaAlert />);

    expect(await screen.findByTestId("mfa-required-alert")).toBeVisible();
  });

  it("does not show the MFA alert to non-admins", async () => {
    vi.mocked(useAuthUser).mockReturnValue({
      ...dummyValidUser,
      // Get rid of Admin role
      roles: [],
    });
    renderComponent(<MfaAlert />);

    expect(screen.queryByTestId("mfa-required-alert")).not.toBeInTheDocument();
  });

  it("does not render the MFA alert when SSO is enabled", async () => {
    vi.mocked(useAuthUser).mockReturnValue({
      ...dummyValidUser,
      managedBy: UserManagedByEnum.SCIM2,
    });
    server.use(dontForceMfaPolicyHandler);

    renderComponent(<MfaAlert />);

    expect(screen.queryByTestId("mfa-required-alert")).not.toBeInTheDocument();
  });

  it("does not render the MFA alert when MFA is already enforced", async () => {
    server.use(forceMfaPolicyHandler);

    renderComponent(<MfaAlert />);

    expect(screen.queryByTestId("mfa-required-alert")).not.toBeInTheDocument();
  });
});
