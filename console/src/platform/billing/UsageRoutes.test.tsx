// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import React from "react";
import { Route, Routes } from "react-router-dom";

import { Organization } from "~/api/cloudGlobalApi";
import {
  buildCloudOrganizationsResponse,
  buildInvoicesResponse,
} from "~/api/mocks/cloudGlobalApiHandlers";
import server from "~/api/mocks/server";
import { CloudRuntimeConfig } from "~/config/AppConfigSwitch";
import { dummyValidUser } from "~/external-library-wrappers/__mocks__/frontegg";
import type {
  ITeamUserPermission,
  User,
} from "~/external-library-wrappers/frontegg";
import {
  createProviderWrapper,
  healthyEnvironment,
  setFakeEnvironment,
} from "~/test/utils";

import UsageRoutes from "./UsageRoutes";

// `useCanViewBilling` requires this flag; its real value lives in
// LaunchDarkly, so mock it to isolate the invoices-based gate under test.
vi.mock("~/hooks/useFlags", () => ({
  useFlags: () => ({ "billing-ui-3756": true }),
}));

// This test only exercises the tab strip / route gating in UsageRoutes
// itself, not the pages' own data fetching, so stub them out rather than
// mocking every endpoint UsagePage and BillingPage otherwise need.
vi.mock("./UsagePage", () => ({
  default: () => <div>Usage Page</div>,
}));
vi.mock("./BillingPage", () => ({
  default: () => <div>Billing Page</div>,
}));

const adminUser: User = {
  ...dummyValidUser,
  permissions: [
    { key: "materialize.environment.write" } as ITeamUserPermission,
    { key: "materialize.environment.read" } as ITeamUserPermission,
    { key: "materialize.invoice.read" } as ITeamUserPermission,
  ],
};

// `auth`/`authActions` aren't exercised by this test (UsageRoutes only reads
// `isImpersonating` and `user`); the shapes below just satisfy the type.
const runtimeConfig = {
  isImpersonating: false as const,
  user: adminUser,
  auth: { tenantsState: { tenants: [] } },
  authActions: { switchTenant: vi.fn() },
} as unknown as CloudRuntimeConfig;

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
  ...overrides,
});

const oneInvoice = [
  {
    issueDate: "2026-06-02T00:00:00Z",
    currency: "usd",
    total: "100.00",
    amountDue: "100.00",
    createdAt: "2026-06-02T00:00:00Z",
    status: "paid",
    invoiceNumber: "INV-001",
  },
];

const renderAt = async (path: string) => {
  const Wrapper = await createProviderWrapper({
    initializeState: ({ set }) =>
      setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
    router: { type: "MEMORY_ROUTER", initialRouterEntries: [path] },
  });
  // Mirrors how AuthenticatedRoutes.tsx mounts UsageRoutes (nested under a
  // "usage" prefix) so the leading-slash paths inside UsageRoutes ("/overview",
  // "/billing") match against the right remainder of the URL.
  return render(
    <Wrapper>
      <Routes>
        <Route
          path="usage/*"
          element={
            <UsageRoutes
              runtimeConfig={runtimeConfig}
              stripePromise={stripePromise}
            />
          }
        />
      </Routes>
    </Wrapper>,
  );
};

describe("UsageRoutes", () => {
  afterEach(() => {
    server.resetHandlers();
  });

  it("shows Billing for a paying org with invoices", async () => {
    server.use(
      buildCloudOrganizationsResponse({ payload: buildOrganization() }),
      buildInvoicesResponse({ invoices: oneInvoice }),
    );
    await renderAt("/usage/overview");

    await waitFor(() => {
      expect(screen.getByRole("link", { name: "Billing" })).toBeVisible();
    });
  });

  it("shows Billing for an evaluation org even with no invoices yet", async () => {
    server.use(
      buildCloudOrganizationsResponse({
        payload: buildOrganization({
          subscription: { type: "evaluation", marketplace: "direct" },
        }),
      }),
      buildInvoicesResponse({ invoices: [] }),
    );
    // Evaluation orgs have no Overview route (see isUpgradedPlan gating).
    await renderAt("/usage/billing");

    await waitFor(() => {
      expect(screen.getByRole("link", { name: "Billing" })).toBeVisible();
    });
  });

  it("keeps the Billing route mounted while invoices are still loading", async () => {
    let resolveInvoices: () => void = () => {};
    server.use(
      buildCloudOrganizationsResponse({ payload: buildOrganization() }),
      http.get("*/api/invoices", () => {
        return new Promise((resolve) => {
          resolveInvoices = () =>
            resolve(HttpResponse.json({ data: oneInvoice, nextCursor: null }));
        });
      }),
    );
    await renderAt("/usage/billing");

    // Fail open while the invoices query is in flight: a paying org
    // refreshing directly on /usage/billing must not see a blank page while
    // this resolves.
    expect(screen.getByText("Billing Page")).toBeVisible();

    resolveInvoices();
    await waitFor(() => {
      expect(screen.getByRole("link", { name: "Billing" })).toBeVisible();
    });
  });

  it("hides Billing for a non-evaluation org with no invoices (e.g. a leaf account)", async () => {
    server.use(
      buildCloudOrganizationsResponse({ payload: buildOrganization() }),
      buildInvoicesResponse({ invoices: [] }),
    );
    await renderAt("/usage/overview");

    // Billing is visible on the very first render, before the mocked
    // (empty) invoices have loaded, so wait for it to disappear.
    await waitFor(() => {
      expect(
        screen.queryByRole("link", { name: "Billing" }),
      ).not.toBeInTheDocument();
    });
    expect(screen.getByRole("link", { name: "Overview" })).toBeVisible();
  });
});
