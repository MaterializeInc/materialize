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

import { Organization } from "~/api/cloudGlobalApi";
import MaterializeErrorCode from "~/api/materialize/errorCodes";
import NetworkPolicyError from "~/api/materialize/NetworkPolicyError";
import { buildCloudOrganizationsResponse } from "~/api/mocks/cloudGlobalApiHandlers";
import server from "~/api/mocks/server";
import { dummyValidUser } from "~/external-library-wrappers/__mocks__/frontegg";
import {
  type ITeamUserPermission,
  useAuthUser,
} from "~/external-library-wrappers/frontegg";
import {
  healthyEnvironment,
  InitializeStateFn,
  renderComponent,
  setFakeEnvironment,
} from "~/test/utils";

import BlockedState from "./BlockedState";

const renderBlockedState = (initializeState: InitializeStateFn) => {
  renderComponent(<BlockedState />, { initializeState });
};

const buildOrganization = (overrides: Partial<Organization> = {}) => {
  return {
    id: "00000000-0000-0000-0000-000000000000",
    name: "Console Unit Test Organization",
    blocked: false,
    onboarded: true,
    trialExpiresAt: null,
    subscription: {
      type: "capacity" as const,
      marketplace: "direct" as const,
    },
    ...overrides,
  };
};

describe("BlockedState", () => {
  beforeEach(() => {
    server.use(
      buildCloudOrganizationsResponse({
        payload: buildOrganization(),
      }),
    );
  });

  afterEach(() => {
    server.resetHandlers();
    vi.clearAllMocks();
  });

  it("displays organization blocked state for paid customers", async () => {
    server.use(
      buildCloudOrganizationsResponse({
        payload: buildOrganization(),
      }),
    );
    renderBlockedState(({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", {
        ...healthyEnvironment,
        status: {
          health: "blocked",
          errors: [],
        },
      }),
    );
    expect(
      await screen.findByText(
        "Your Materialize plan has lapsed or is upgrading",
      ),
    ).toBeVisible();
  });

  it("displays organization blocked state for trial customers", async () => {
    server.use(
      buildCloudOrganizationsResponse({
        payload: buildOrganization({
          subscription: {
            type: "evaluation" as const,
            marketplace: "direct" as const,
          },
        }),
      }),
    );
    renderBlockedState(({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", {
        ...healthyEnvironment,
        status: {
          health: "blocked",
          errors: [],
        },
      }),
    );
    expect(await screen.findByText("Your trial has ended")).toBeVisible();
  });

  it("displays a network policy blocked state for IPv4 users", async () => {
    renderBlockedState(({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", {
        ...healthyEnvironment,
        status: {
          health: "blocked",
          errors: [
            {
              message: "Environment blocked",
              details: new NetworkPolicyError(
                MaterializeErrorCode.NETWORK_POLICY_SESSION_DENIED,
                "Access denied for address 127.0.0.1",
              ),
            },
          ],
        },
      }),
    );
    expect(await screen.findByText("Connection blocked")).toBeVisible();
    expect(await screen.findByText("127.0.0.1")).toBeVisible();
  });

  it("displays a network policy blocked state for IPv6 users", async () => {
    renderBlockedState(({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", {
        ...healthyEnvironment,
        status: {
          health: "blocked",
          errors: [
            {
              message: "Environment blocked",
              details: new NetworkPolicyError(
                MaterializeErrorCode.NETWORK_POLICY_SESSION_DENIED,
                "Access denied for address 0:0:0:0:0:0:0:1",
              ),
            },
          ],
        },
      }),
    );
    expect(await screen.findByText("Connection blocked")).toBeVisible();
    expect(await screen.findByText("0:0:0:0:0:0:0:1")).toBeVisible();
  });

  it("displays a network policy blocked state without an IP address when one cannot be parsed from a message", async () => {
    renderBlockedState(({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", {
        ...healthyEnvironment,
        status: {
          health: "blocked",
          errors: [
            {
              message: "Environment blocked",
              details: new NetworkPolicyError(
                MaterializeErrorCode.NETWORK_POLICY_SESSION_DENIED,
                "Access denied for address 123 Fake Street",
              ),
            },
          ],
        },
      }),
    );
    expect(await screen.findByText("Connection blocked")).toBeVisible();
    expect(
      await screen.findByText("Your IP address is not included", {
        exact: false,
      }),
    ).toBeVisible();
  });

  it("displays a support link for admin users blocked by network policy", async () => {
    vi.mocked(useAuthUser).mockReturnValue({
      ...dummyValidUser,
      permissions: [
        {
          key: "materialize.environment.write",
        } as ITeamUserPermission,
      ],
    });
    renderBlockedState(({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", {
        ...healthyEnvironment,
        status: {
          health: "blocked",
          errors: [
            {
              message: "Environment blocked",
              details: new NetworkPolicyError(
                MaterializeErrorCode.NETWORK_POLICY_SESSION_DENIED,
                "Access denied for address 127.0.0.1",
              ),
            },
          ],
        },
      }),
    );
    expect(await screen.findByTestId("admin-support-link")).toBeVisible();
  });

  it("points non-admin users to their admins when blocked by network policy", async () => {
    vi.mocked(useAuthUser).mockReturnValue({
      ...dummyValidUser,
      permissions: [],
    });

    renderBlockedState(({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", {
        ...healthyEnvironment,
        status: {
          health: "blocked",
          errors: [
            {
              message: "Environment blocked",
              details: new NetworkPolicyError(
                MaterializeErrorCode.NETWORK_POLICY_SESSION_DENIED,
                "Access denied for address 127.0.0.1",
              ),
            },
          ],
        },
      }),
    );
    expect(
      await screen.findByText("your administrators", { exact: false }),
    ).toBeVisible();
  });
});
