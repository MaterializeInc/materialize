// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import React, { ReactElement } from "react";

import { CostBreakdownAccount, Organization } from "~/api/cloudGlobalApi";
import {
  buildCloudOrganizationsResponse,
  buildCloudRegionsReponse,
  buildCreditsResponse,
  buildDailyCostBreakdownResponse,
  buildInvoicesResponse,
} from "~/api/mocks/cloudGlobalApiHandlers";
import server from "~/api/mocks/server";
import {
  createProviderWrapper,
  healthyEnvironment,
  selectReactSelectOption,
  setFakeEnvironment,
} from "~/test/utils";
import { formatCurrency } from "~/utils/format";

import { getDayAlignedRange } from "./queries";
import UsagePage from "./UsagePage";

const Wrapper = await createProviderWrapper({
  initializeState: ({ set }) =>
    setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
});

const renderComponent = (element: ReactElement) => {
  return render(<Wrapper>{element}</Wrapper>);
};

// Wrap accounts in a single UTC-day bucket — the minimal `/api/costs/breakdown/
// daily` payload. Per-day cost is additive, so a one-day window reproduces the
// period totals the breakdown asserts on.
const oneDay = (accounts: CostBreakdownAccount[]) => [
  {
    startDate: "2024-01-15T00:00:00Z",
    endDate: "2024-01-16T00:00:00Z",
    accounts,
  },
];

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

describe("UsagePage", () => {
  beforeEach(() => {
    server.use(
      buildCloudRegionsReponse(),
      buildInvoicesResponse(),
      buildCreditsResponse(),
      buildDailyCostBreakdownResponse(),
      buildCloudOrganizationsResponse({
        payload: buildOrganization(),
      }),
    );
  });

  afterEach(() => {
    server.resetHandlers();
    vi.clearAllMocks();
  });

  it("renders a unified account & cluster ledger for a parent org", async () => {
    server.use(
      buildDailyCostBreakdownResponse({
        payload: {
          days: oneDay([
            {
              external_customer_id: "parent-org",
              name: "Built-Prod",
              clusters: [
                {
                  environment_id: "environment-parent-0",
                  cluster_grouping_key: "quickstart.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "10.00" },
                  usage: 0,
                },
                {
                  environment_id: "environment-parent-0",
                  cluster_grouping_key: "compute.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "4.00" },
                  usage: 0,
                },
              ],
            },
            {
              external_customer_id: "child-org",
              name: "",
              clusters: [
                {
                  environment_id: "environment-child-0",
                  cluster_grouping_key: "prod.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "5.00" },
                  usage: 0,
                },
              ],
            },
          ]),
        },
      }),
    );
    renderComponent(<UsagePage />);

    const breakdown = within(
      await screen.findByTestId(
        "account-spend-breakdown",
        {},
        { timeout: 5_000 },
      ),
    );
    // The ledger lives in its own full-width grid row below the chart and
    // plan-details columns (SAS-154).
    const ledger = within(await screen.findByTestId("account-spend-ledger"));
    // One expandable row per account, biggest spender first, each showing that
    // account's period total (parent 14 = 10 + 4, child 5).
    const accountRows = await ledger.findAllByTestId("account-row");
    expect(accountRows).toHaveLength(2);
    expect(within(accountRows[0]).getByText(formatCurrency(14))).toBeVisible();
    expect(within(accountRows[1]).getByText(formatCurrency(5))).toBeVisible();
    // The parent has a display name (SAS-142) and renders it in place of the
    // UUID; the child has none, so its row falls back to the short UUID.
    expect(within(accountRows[0]).getByText("Built-Prod")).toBeVisible();
    expect(within(accountRows[1]).getByText("child-or…")).toBeVisible();
    // Each account row also shows its share of the period total (SAS-144):
    // parent 14/19 ≈ 73.7%, child 5/19 ≈ 26.3%.
    expect(within(accountRows[0]).getByText("73.7%")).toBeVisible();
    expect(within(accountRows[1]).getByText("26.3%")).toBeVisible();
    // With every account collapsed, the Usage column header has nothing
    // beneath it to label, so it's hidden (SAS-169).
    expect(ledger.queryByText("Usage")).not.toBeInTheDocument();
    // Accounts render collapsed by default (SAS-168); expand both to reveal
    // their cluster rows, region-qualified ("aws/us-east-1 / <cluster>").
    await userEvent.click(accountRows[0]);
    await userEvent.click(accountRows[1]);
    for (const cluster of ["quickstart.r1", "compute.r1", "prod.r1"]) {
      expect(
        await ledger.findByText(`aws/us-east-1 / ${cluster}`),
      ).toBeVisible();
    }
    // The segmented-by-account chart and the grand-total row (19 = 14 + 5) are
    // present.
    expect(await breakdown.findByTestId("account-spend-chart")).toBeVisible();
    const totalRow = within(await ledger.findByTestId("account-total-row"));
    expect(totalRow.getByText(formatCurrency(19))).toBeVisible();
    // A Usage column sits between "Account / cluster" and "Share of total"
    // (SAS-145/SAS-159): each cluster row shows its usage quantity. With at
    // least one account expanded, the header is shown again (SAS-169).
    expect(await ledger.findByText("Usage")).toBeVisible();
    expect(ledger.getAllByText("0 credits")).toHaveLength(3);
    // Collapsing every account back closed hides the header again.
    await userEvent.click(accountRows[0]);
    await userEvent.click(accountRows[1]);
    await waitFor(() => {
      expect(ledger.queryByText("Usage")).not.toBeInTheDocument();
    });
    // The section leads with the period total (19 = 14 + 5), mirroring the
    // legacy chart panel, and a "Spend between …" range above the table,
    // mirroring the legacy "Spend between …" breakdown. oneDay()'s single
    // bucket is 2024-01-15, so the range collapses to that one date.
    expect(
      within(await breakdown.findByTestId("account-spend-total")).getByText(
        formatCurrency(19),
      ),
    ).toBeVisible();
    const range = within(await ledger.findByTestId("account-spend-range"));
    expect(range.getByText("Spend between", { exact: false })).toBeVisible();
    expect(range.getAllByText("01-15-24")).toHaveLength(2);
  });

  it("sends bare inclusive UTC calendar dates to the breakdown endpoint (SAS-151)", async () => {
    // Regression guard for the timestamp-serialization bug class: the query
    // params must be plain YYYY-MM-DD (no time component, no local-zone
    // offset for a shifted wall-clock date to hide in), computed from UTC
    // calendar fields, both ends inclusive.
    const captured: URLSearchParams[] = [];
    server.use(
      http.get("*/api/costs/breakdown/daily", ({ request }) => {
        captured.push(new URL(request.url).searchParams);
        return HttpResponse.json({ days: [] });
      }),
    );
    // Computed before render so a UTC-midnight rollover mid-test can't skew
    // the expectation.
    const now = new Date();
    const utcDay = (offset: number) =>
      new Date(
        Date.UTC(
          now.getUTCFullYear(),
          now.getUTCMonth(),
          now.getUTCDate() + offset,
        ),
      )
        .toISOString()
        .slice(0, 10);

    renderComponent(<UsagePage />);
    // The page fires two breakdown queries: the selected range (default
    // "Last 7 days") and the fixed 30-day plan-details window.
    await waitFor(() => expect(captured.length).toBeGreaterThanOrEqual(2));

    for (const params of captured) {
      expect(params.get("startDate")).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      expect(params.get("endDate")).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    }
    // "Last N days" is the N inclusive UTC days ending today, so exactly N
    // buckets can come back — the 31-buckets-for-30-days symptom is pinned
    // out here.
    const sevenDay = captured.find((p) => p.get("startDate") === utcDay(-6));
    expect(sevenDay?.get("endDate")).toBe(utcDay(0));
    const thirtyDay = captured.find((p) => p.get("startDate") === utcDay(-29));
    expect(thirtyDay?.get("endDate")).toBe(utcDay(0));
  });

  it("shows an error state when the breakdown request fails", async () => {
    server.use(buildDailyCostBreakdownResponse({ status: 500 }));
    renderComponent(<UsagePage />);
    expect(
      await screen.findByText("An error occurred loading your usage"),
    ).toBeVisible();
  });

  it("shows an empty state when the window has no usage", async () => {
    // beforeEach's default handler already returns { days: [] }.
    renderComponent(<UsagePage />);
    expect(await screen.findByTestId("account-breakdown-empty")).toBeVisible();
  });

  it("renders 0.0% shares for an all-zero period instead of NaN (SAS-144)", async () => {
    server.use(
      buildDailyCostBreakdownResponse({
        payload: {
          days: oneDay([
            {
              external_customer_id: "parent-org",
              name: "",
              clusters: [
                {
                  environment_id: "environment-parent-0",
                  cluster_grouping_key: "quickstart.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "0.00" },
                  usage: 0,
                },
              ],
            },
            {
              external_customer_id: "child-org",
              name: "",
              clusters: [
                {
                  environment_id: "environment-child-0",
                  cluster_grouping_key: "prod.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "0.00" },
                  usage: 0,
                },
              ],
            },
          ]),
        },
      }),
    );
    renderComponent(<UsagePage />);

    const ledger = within(
      await screen.findByTestId("account-spend-ledger", {}, { timeout: 5_000 }),
    );
    const accountRows = await ledger.findAllByTestId("account-row");
    expect(accountRows).toHaveLength(2);
    for (const row of accountRows) {
      expect(within(row).getByText("0.0%")).toBeVisible();
    }
    expect(ledger.queryByText(/NaN/)).not.toBeInTheDocument();
  });

  it("shows a plan-details box beside the breakdown, itemizing last 30 days by account", async () => {
    server.use(
      buildDailyCostBreakdownResponse({
        payload: {
          days: oneDay([
            {
              external_customer_id: "parent-org",
              name: "",
              clusters: [
                {
                  environment_id: "environment-parent-0",
                  cluster_grouping_key: "quickstart.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "10.00" },
                  usage: 0,
                },
                {
                  environment_id: "environment-parent-0",
                  cluster_grouping_key: "compute.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "4.00" },
                  usage: 0,
                },
              ],
            },
            {
              external_customer_id: "child-org",
              name: "",
              clusters: [
                {
                  environment_id: "environment-child-0",
                  cluster_grouping_key: "prod.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "5.00" },
                  usage: 0,
                },
              ],
            },
          ]),
        },
      }),
    );
    renderComponent(<UsagePage />);

    const planDetails = within(
      await screen.findByTestId("account-plan-details", {}, { timeout: 5_000 }),
    );
    // The box renders before the breakdown resolves, so await the spend rows.
    // Every figure is derived from /api/costs/breakdown/daily, not
    // /api/costs/daily.
    expect(await planDetails.findByText("Total spend")).toBeVisible();
    expect(await planDetails.findByText("Last 30 days")).toBeVisible();
    expect(await planDetails.findByText("Daily average")).toBeVisible();
    // "Last 30 days" is itemized by account (parent 14 = 10 + 4, child 5),
    // biggest spender first, under the window total.
    expect(await planDetails.findByText(formatCurrency(14))).toBeVisible();
    expect(await planDetails.findByText(formatCurrency(5))).toBeVisible();
  });

  it("renders storage and egress as distinct region-qualified rows, each with its own usage unit (SAS-159)", async () => {
    server.use(
      buildDailyCostBreakdownResponse({
        payload: {
          days: oneDay([
            {
              external_customer_id: "standalone-org",
              name: "",
              clusters: [
                {
                  environment_id: "environment-standalone-0",
                  cluster_grouping_key: "default.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "3.00" },
                  usage: 2.5,
                },
                {
                  // Storage and egress both have an empty cluster_grouping_key;
                  // their `category` keeps them on separate rows, rendered as
                  // "<region> / Storage" and "<region> / Egress".
                  environment_id: "environment-standalone-0",
                  cluster_grouping_key: "",
                  category: "Storage",
                  region: "aws/us-east-1",
                  amounts: { "price-storage": "0.50" },
                  usage: 10.75,
                },
                {
                  environment_id: "environment-standalone-0",
                  cluster_grouping_key: "",
                  category: "Egress",
                  region: "aws/us-east-1",
                  amounts: { "price-egress": "0.25" },
                  usage: 3,
                },
              ],
            },
          ]),
        },
      }),
    );
    renderComponent(<UsagePage />);

    const ledger = within(
      await screen.findByTestId("account-spend-ledger", {}, { timeout: 5_000 }),
    );
    // Accounts render collapsed by default (SAS-168); expand to reveal its
    // clusters. Scope lookups to the ledger table. Storage and egress (both
    // empty cluster_grouping_key) stay on separate rows via `category`.
    await userEvent.click(await ledger.findByTestId("account-row"));
    await waitFor(() => {
      expect(ledger.getByText("aws/us-east-1 / default.r1")).toBeVisible();
      expect(ledger.getByText("aws/us-east-1 / Storage")).toBeVisible();
      expect(ledger.getByText("aws/us-east-1 / Egress")).toBeVisible();
    });
    // The compute row's usage renders in credits, the storage/egress rows'
    // in GB.
    expect(ledger.getByText("2.5 credits")).toBeVisible();
    expect(ledger.getByText("10.75 GB")).toBeVisible();
    expect(ledger.getByText("3 GB")).toBeVisible();
  });

  it("falls back to 'Other' when a row has neither cluster key nor category", async () => {
    // Defensive: a non-compute row with an empty cluster_grouping_key and no
    // category can only occur against a backend that predates the `category`
    // field. It should render "<region> / Other" rather than mislabel as a
    // cluster or crash.
    server.use(
      buildDailyCostBreakdownResponse({
        payload: {
          days: oneDay([
            {
              external_customer_id: "standalone-org",
              name: "",
              clusters: [
                {
                  environment_id: "environment-standalone-0",
                  cluster_grouping_key: "",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-storage": "0.50" },
                  usage: 0,
                },
              ],
            },
          ]),
        },
      }),
    );
    renderComponent(<UsagePage />);

    const ledger = within(
      await screen.findByTestId("account-spend-ledger", {}, { timeout: 5_000 }),
    );
    // Accounts render collapsed by default (SAS-168); expand to reveal its
    // one cluster row.
    await userEvent.click(await ledger.findByTestId("account-row"));
    await waitFor(() => {
      expect(ledger.getByText("aws/us-east-1 / Other")).toBeVisible();
    });
  });

  it("filters the ledger by region", async () => {
    server.use(
      buildDailyCostBreakdownResponse({
        payload: {
          days: oneDay([
            {
              external_customer_id: "east-org",
              name: "",
              clusters: [
                {
                  environment_id: "environment-east-0",
                  cluster_grouping_key: "quickstart.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "10.00" },
                  usage: 0,
                },
              ],
            },
            {
              external_customer_id: "west-org",
              name: "",
              clusters: [
                {
                  environment_id: "environment-west-0",
                  cluster_grouping_key: "quickstart.r1",
                  category: "",
                  region: "aws/eu-west-1",
                  amounts: { "price-compute": "5.00" },
                  usage: 0,
                },
              ],
            },
          ]),
        },
      }),
    );
    renderComponent(<UsagePage />);

    const ledger = within(
      await screen.findByTestId("account-spend-ledger", {}, { timeout: 5_000 }),
    );
    // Both accounts show before filtering.
    expect(await ledger.findAllByTestId("account-row")).toHaveLength(2);
    const totalRowBefore = within(
      await ledger.findByTestId("account-total-row"),
    );
    expect(totalRowBefore.getByText(formatCurrency(15))).toBeVisible();

    const planDetails = within(
      await screen.findByTestId("account-plan-details", {}, { timeout: 5_000 }),
    );
    const totalSpendLabel = await planDetails.findByText("Total spend");
    // The plan-details summary must track the same region filter as the
    // ledger, so it starts at the unfiltered $15 total too.
    expect(
      within(totalSpendLabel.parentElement as HTMLElement).getByText(
        formatCurrency(15),
      ),
    ).toBeVisible();

    const regionSelect = screen.getByTestId<HTMLElement>(
      "account-region-select",
    );
    await selectReactSelectOption(regionSelect, "aws/us-east-1");

    // Only the us-east-1 account remains, and the grand total drops to just
    // its cost — the eu-west-1 account is filtered out entirely.
    await waitFor(async () => {
      expect(await ledger.findAllByTestId("account-row")).toHaveLength(1);
    });
    const accountRows = await ledger.findAllByTestId("account-row");
    expect(within(accountRows[0]).getByText(formatCurrency(10))).toBeVisible();
    const totalRowAfter = within(
      await ledger.findByTestId("account-total-row"),
    );
    expect(totalRowAfter.getByText(formatCurrency(10))).toBeVisible();

    // The plan-details summary follows the same filter: total spend drops to
    // the us-east-1-only figure, matching the ledger.
    await waitFor(() => {
      expect(
        within(totalSpendLabel.parentElement as HTMLElement).getByText(
          formatCurrency(10),
        ),
      ).toBeVisible();
    });
  });

  it("displays the invoices table for direct-billed organizations", async () => {
    server.use(
      buildInvoicesResponse({
        invoices: [
          {
            issueDate: "2026-06-02T00:00:00Z",
            currency: "usd",
            total: "100.00",
            amountDue: "100.00",
            createdAt: "2026-06-02T00:00:00Z",
            status: "paid",
            invoiceNumber: "INV-001",
          },
        ],
      }),
    );
    renderComponent(<UsagePage />);
    await waitFor(async () =>
      expect(await screen.findByTestId("invoice-table")).toBeVisible(),
    );
  });

  it("hides the invoice history section entirely for an account with no invoices (e.g. an Orb hierarchy leaf account, which never has its own)", async () => {
    server.use(buildInvoicesResponse({ invoices: [] }));
    renderComponent(<UsagePage />);
    await waitFor(async () =>
      expect(
        await screen.findByTestId("account-spend-breakdown"),
      ).toBeVisible(),
    );
    expect(screen.queryByText("Invoice history")).not.toBeInTheDocument();
    expect(screen.queryByTestId("invoice-table")).not.toBeInTheDocument();
  });

  it("hides the invoices table for AWS Marketplace-billed organizations", async () => {
    server.use(
      buildCloudOrganizationsResponse({
        payload: buildOrganization({
          subscription: {
            type: "capacity" as const,
            marketplace: "aws" as const,
          },
        }),
      }),
    );
    renderComponent(<UsagePage />);

    await waitFor(async () =>
      expect(await screen.findByTestId("aws-marketplace-banner")).toBeVisible(),
    );
  });

  it("generates appropriate time components for a time range", () => {
    const [start, end] = getDayAlignedRange(7);
    expect(start.getUTCHours()).toEqual(0);
    expect(start.getUTCHours()).toEqual(0);
    expect(start.getUTCMinutes()).toEqual(0);
    expect(start.getUTCSeconds()).toEqual(0);
    expect(start.getUTCMilliseconds()).toEqual(0);
    expect(end.getUTCHours()).toEqual(0);
    expect(end.getUTCMinutes()).toEqual(0);
    expect(end.getUTCSeconds()).toEqual(0);
    expect(end.getUTCMilliseconds()).toEqual(0);
  });
});
