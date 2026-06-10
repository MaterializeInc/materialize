// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { screen, waitFor } from "@testing-library/react";
import React from "react";

import { Organization } from "~/api/cloudGlobalApi";
import { buildCloudOrganizationsResponse } from "~/api/mocks/cloudGlobalApiHandlers";
import server from "~/api/mocks/server";
import { dummyValidUser } from "~/external-library-wrappers/__mocks__/frontegg";
import type {
  ITeamUserPermission,
  User,
} from "~/external-library-wrappers/frontegg";
import { renderComponent } from "~/test/utils";

import BillingPage from "./BillingPage";

const adminUser: User = {
  ...dummyValidUser,
  permissions: [
    { key: "materialize.environment.write" } as ITeamUserPermission,
    { key: "materialize.environment.read" } as ITeamUserPermission,
    { key: "materialize.invoice.read" } as ITeamUserPermission,
  ],
};

const nonAdminUser: User = {
  ...dummyValidUser,
  permissions: [
    { key: "materialize.environment.read" } as ITeamUserPermission,
    { key: "materialize.invoice.read" } as ITeamUserPermission,
  ],
  roles: [],
};

const stripePromise = Promise.resolve(null);

const buildOrganization = (
  overrides: Partial<Organization> = {},
): Organization => ({
  id: "org_id",
  name: "Test Organization",
  blocked: false,
  onboarded: true,
  trialExpiresAt: null,
  subscription: { type: "on-demand", marketplace: "direct" },
  paymentMethods: [
    {
      id: "pm_1",
      type: "card",
      card: { last4: "4242", expMonth: 12, expYear: 2027 },
      created: 1700000000,
      defaultPaymentMethod: true,
    },
    {
      id: "pm_2",
      type: "card",
      card: { last4: "1234", expMonth: 6, expYear: 2028 },
      created: 1700000001,
      defaultPaymentMethod: false,
    },
  ],
  ...overrides,
});

describe("BillingPage", () => {
  afterEach(() => {
    server.resetHandlers();
  });

  describe("admin user", () => {
    it("shows payment management actions", async () => {
      server.use(
        buildCloudOrganizationsResponse({ payload: buildOrganization() }),
      );

      await renderComponent(
        <BillingPage user={adminUser} stripePromise={stripePromise} />,
      );

      await waitFor(() => {
        expect(screen.getByText("Card ending in 4242")).toBeVisible();
      });

      expect(screen.getByText("Set as default")).toBeVisible();
      expect(screen.getAllByText("Delete")).toHaveLength(2);
    });

    it("shows add payment method button when under the limit", async () => {
      server.use(
        buildCloudOrganizationsResponse({
          payload: buildOrganization({
            paymentMethods: [
              {
                id: "pm_1",
                type: "card",
                card: { last4: "4242", expMonth: 12, expYear: 2027 },
                created: 1700000000,
                defaultPaymentMethod: true,
              },
            ],
          }),
        }),
      );

      await renderComponent(
        <BillingPage user={adminUser} stripePromise={stripePromise} />,
      );

      await waitFor(() => {
        expect(screen.getByText("Card ending in 4242")).toBeVisible();
      });

      expect(screen.getByText("Add payment method")).toBeVisible();
    });

    it("does not show the non-admin info alert", async () => {
      server.use(
        buildCloudOrganizationsResponse({ payload: buildOrganization() }),
      );

      await renderComponent(
        <BillingPage user={adminUser} stripePromise={stripePromise} />,
      );

      await waitFor(() => {
        expect(screen.getByText("Card ending in 4242")).toBeVisible();
      });

      expect(
        screen.queryByText(/Only organization admins can manage/),
      ).not.toBeInTheDocument();
    });
  });

  describe("non-admin user", () => {
    it("hides payment management actions", async () => {
      server.use(
        buildCloudOrganizationsResponse({ payload: buildOrganization() }),
      );

      await renderComponent(
        <BillingPage user={nonAdminUser} stripePromise={stripePromise} />,
      );

      await waitFor(() => {
        expect(screen.getByText("Card ending in 4242")).toBeVisible();
      });

      // Cards are visible but mutation actions are hidden
      expect(screen.getByText("Card ending in 1234")).toBeVisible();
      expect(screen.queryByText("Set as default")).not.toBeInTheDocument();
      expect(screen.queryByText("Delete")).not.toBeInTheDocument();
      expect(screen.queryByText("Add payment method")).not.toBeInTheDocument();
    });

    it("shows an info alert directing the user to contact an admin", async () => {
      server.use(
        buildCloudOrganizationsResponse({ payload: buildOrganization() }),
      );

      await renderComponent(
        <BillingPage user={nonAdminUser} stripePromise={stripePromise} />,
      );

      expect(
        await screen.findByText(
          "Only organization admins can manage payment methods. Contact an admin to make changes.",
        ),
      ).toBeVisible();
    });

    it("shows contact admin text in the evaluation CTA banner", async () => {
      const trialExpiresAt = new Date();
      trialExpiresAt.setDate(trialExpiresAt.getDate() + 7);

      server.use(
        buildCloudOrganizationsResponse({
          payload: buildOrganization({
            subscription: { type: "evaluation", marketplace: "direct" },
            trialExpiresAt: trialExpiresAt.toISOString(),
            paymentMethods: [],
          }),
        }),
      );

      await renderComponent(
        <BillingPage user={nonAdminUser} stripePromise={stripePromise} />,
      );

      const contactMessages = await screen.findAllByText(
        "Contact an organization admin to upgrade.",
      );
      expect(contactMessages.length).toBeGreaterThanOrEqual(1);
    });

    it("shows contact admin text instead of Upgrade button in plan details", async () => {
      server.use(
        buildCloudOrganizationsResponse({
          payload: buildOrganization({
            subscription: { type: "evaluation", marketplace: "direct" },
            paymentMethods: [],
          }),
        }),
      );

      await renderComponent(
        <BillingPage user={nonAdminUser} stripePromise={stripePromise} />,
      );

      await waitFor(() => {
        expect(screen.getByText("Plan details")).toBeVisible();
      });

      expect(
        screen.queryByRole("button", { name: /Upgrade/i }),
      ).not.toBeInTheDocument();
    });
  });
});
