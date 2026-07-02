// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen, waitFor, within } from "@testing-library/react";
import React, { ReactElement } from "react";

import {
  CostBreakdownAccount,
  DailyCosts,
  Organization,
} from "~/api/cloudGlobalApi";
import {
  buildCloudOrganizationsResponse,
  buildCloudRegionsReponse,
  buildCreditsResponse,
  buildDailyCostBreakdownResponse,
  buildDailyCostResponse,
  buildInvoicesResponse,
  generateDailyCostResponsePayload,
} from "~/api/mocks/cloudGlobalApiHandlers";
import server from "~/api/mocks/server";
import {
  createProviderWrapper,
  healthyEnvironment,
  selectReactSelectOption,
  setFakeEnvironment,
} from "~/test/utils";
import { assert } from "~/util";
import { formatCurrency } from "~/utils/format";

import { getTimeRange } from "./queries";
import UsagePage from "./UsagePage";
import { getTimeRangeSlice } from "./utils";

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

  it("renders successfully for all regions", async () => {
    const [startDate, endDate] = getTimeRange(7);
    const payload = generateDailyCostResponsePayload(startDate, endDate);
    expect(payload.daily.length).toEqual(30);
    server.use(buildDailyCostResponse({ payload }));
    renderComponent(<UsagePage />);

    const regionSelect = await screen.findByTestId(
      "region-select",
      {},
      { timeout: 5_000 },
    );
    expect(regionSelect).toBeVisible();

    const timeRangeSelect = await screen.findByTestId("time-range-select");
    expect(timeRangeSelect).toBeVisible();

    const spend = await screen.findByTestId("all-spend-amount");
    const timeRangeSubtotal = payload.daily
      .slice(-7)
      .reduce((subtotal, day) => subtotal + parseFloat(day.subtotal), 0);
    expect(spend.textContent).toEqual(formatCurrency(timeRangeSubtotal));

    await waitFor(async () =>
      expect(await screen.findByTestId("chart")).toBeVisible(),
    );
  });

  it("renders a unified account & cluster ledger for a parent org", async () => {
    server.use(buildDailyCostResponse());
    server.use(
      buildDailyCostBreakdownResponse({
        payload: {
          days: oneDay([
            {
              external_customer_id: "parent-org",
              clusters: [
                {
                  environment_id: "environment-parent-0",
                  cluster_grouping_key: "quickstart.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "10.00" },
                },
                {
                  environment_id: "environment-parent-0",
                  cluster_grouping_key: "compute.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "4.00" },
                },
              ],
            },
            {
              external_customer_id: "child-org",
              clusters: [
                {
                  environment_id: "environment-child-0",
                  cluster_grouping_key: "prod.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "5.00" },
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
    // One expandable row per account, biggest spender first, each showing that
    // account's period total (parent 14 = 10 + 4, child 5).
    const accountRows = await breakdown.findAllByTestId("account-row");
    expect(accountRows).toHaveLength(2);
    expect(within(accountRows[0]).getByText(formatCurrency(14))).toBeVisible();
    expect(within(accountRows[1]).getByText(formatCurrency(5))).toBeVisible();
    // Accounts render expanded, so every cluster row is visible inline,
    // region-qualified ("aws/us-east-1 / <cluster>").
    for (const cluster of ["quickstart.r1", "compute.r1", "prod.r1"]) {
      expect(
        await breakdown.findByText(`aws/us-east-1 / ${cluster}`),
      ).toBeVisible();
    }
    // The segmented-by-account chart and the grand-total row (19 = 14 + 5) are
    // present.
    expect(await breakdown.findByTestId("account-spend-chart")).toBeVisible();
    const totalRow = within(await breakdown.findByTestId("account-total-row"));
    expect(totalRow.getByText(formatCurrency(19))).toBeVisible();
    // A Usage column sits between "Account / cluster" and "Share of total"
    // (SAS-145): the endpoint doesn't carry usage quantities yet, so each of
    // the 3 cluster rows shows a placeholder rather than a number.
    expect(await breakdown.findByText("Usage")).toBeVisible();
    expect(breakdown.getAllByText("—")).toHaveLength(3);
    // The section leads with the period total (19 = 14 + 5), mirroring the
    // legacy chart panel, and a "Spend between …" range above the table,
    // mirroring the legacy "Spend between …" breakdown. oneDay()'s single
    // bucket is 2024-01-15, so the range collapses to that one date.
    expect(
      within(await breakdown.findByTestId("account-spend-total")).getByText(
        formatCurrency(19),
      ),
    ).toBeVisible();
    const range = within(await breakdown.findByTestId("account-spend-range"));
    expect(range.getByText("Spend between", { exact: false })).toBeVisible();
    expect(range.getAllByText("01-15-24")).toHaveLength(2);
  });

  it("shows a plan-details box beside the breakdown, itemizing last 30 days by account", async () => {
    server.use(buildDailyCostResponse());
    server.use(
      buildDailyCostBreakdownResponse({
        payload: {
          days: oneDay([
            {
              external_customer_id: "parent-org",
              clusters: [
                {
                  environment_id: "environment-parent-0",
                  cluster_grouping_key: "quickstart.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "10.00" },
                },
                {
                  environment_id: "environment-parent-0",
                  cluster_grouping_key: "compute.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "4.00" },
                },
              ],
            },
            {
              external_customer_id: "child-org",
              clusters: [
                {
                  environment_id: "environment-child-0",
                  cluster_grouping_key: "prod.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "5.00" },
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

  it("renders storage and egress as distinct region-qualified rows", async () => {
    server.use(buildDailyCostResponse());
    server.use(
      buildDailyCostBreakdownResponse({
        payload: {
          days: oneDay([
            {
              external_customer_id: "standalone-org",
              clusters: [
                {
                  environment_id: "environment-standalone-0",
                  cluster_grouping_key: "default.r1",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-compute": "3.00" },
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
                },
                {
                  environment_id: "environment-standalone-0",
                  cluster_grouping_key: "",
                  category: "Egress",
                  region: "aws/us-east-1",
                  amounts: { "price-egress": "0.25" },
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
    // The account renders expanded, so its clusters are visible inline. Scope
    // lookups to this table: SpendBreakdown ("Spend between …") renders the same
    // "<region> / Storage" text from the daily costs. Storage and egress (both
    // empty cluster_grouping_key) stay on separate rows via `category`.
    expect(
      await breakdown.findByText("aws/us-east-1 / default.r1"),
    ).toBeVisible();
    expect(await breakdown.findByText("aws/us-east-1 / Storage")).toBeVisible();
    expect(await breakdown.findByText("aws/us-east-1 / Egress")).toBeVisible();
  });

  it("falls back to 'Other' when a row has neither cluster key nor category", async () => {
    // Defensive: a non-compute row with an empty cluster_grouping_key and no
    // category can only occur against a backend that predates the `category`
    // field. It should render "<region> / Other" rather than mislabel as a
    // cluster or crash.
    server.use(buildDailyCostResponse());
    server.use(
      buildDailyCostBreakdownResponse({
        payload: {
          days: oneDay([
            {
              external_customer_id: "standalone-org",
              clusters: [
                {
                  environment_id: "environment-standalone-0",
                  cluster_grouping_key: "",
                  category: "",
                  region: "aws/us-east-1",
                  amounts: { "price-storage": "0.50" },
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
    // The account renders expanded, so its one cluster row is visible inline.
    expect(await breakdown.findByText("aws/us-east-1 / Other")).toBeVisible();
  });

  it("changing the region filters the totals", async () => {
    const [startDate, endDate] = getTimeRange(7);
    const payload = generateDailyCostResponsePayload(startDate, endDate);
    server.use(buildDailyCostResponse({ payload }));
    renderComponent(<UsagePage />);

    const regionSelect = screen.getByTestId<HTMLElement>("region-select");
    expect(regionSelect).toBeVisible();
    await waitFor(async () =>
      expect(await screen.findByTestId("chart")).toBeVisible(),
    );
    const spendElement = await screen.findByTestId("all-spend-amount");
    const allSpend = parseFloat(
      spendElement.textContent!.replace("$", "").replace(",", ""),
    );
    await selectReactSelectOption(regionSelect, "aws/us-east-1");
    const regionElement = await screen.findByTestId(
      "aws/us-east-1-spend-amount",
    );
    const regionSpend = parseFloat(
      regionElement.textContent!.replace("$", "").replace(",", ""),
    );
    expect(regionSpend, JSON.stringify(payload)).toBeLessThan(allSpend);
  });

  it("changing the time range filters the totals", async () => {
    let [startDate, endDate] = getTimeRange(7);
    let payload = generateDailyCostResponsePayload(startDate, endDate);
    server.use(buildDailyCostResponse({ payload }));
    renderComponent(<UsagePage />);

    const timeRangeSelect =
      screen.getByTestId<HTMLElement>("time-range-select");
    expect(timeRangeSelect).toBeVisible();
    await waitFor(async () =>
      expect(await screen.findByTestId("chart")).toBeVisible(),
    );

    const spendElement = await screen.findByTestId("all-spend-amount");
    const originalSpend = parseFloat(
      spendElement.textContent!.replace("$", "").replace(",", ""),
    );

    [startDate, endDate] = getTimeRange(14);
    payload = generateDailyCostResponsePayload(startDate, endDate);
    server.use(buildDailyCostResponse({ payload }));
    await selectReactSelectOption(timeRangeSelect, "Last 14 days");
    await waitFor(async () =>
      expect(await screen.findByTestId("chart")).toBeVisible(),
    );
    const newSpend = parseFloat(
      spendElement.textContent!.replace("$", "").replace(",", ""),
    );
    expect(originalSpend).toBeLessThan(newSpend);
  });

  it("displays a generic error if there's an issue querying the server", async () => {
    server.use(buildDailyCostResponse({ status: 500 }));
    renderComponent(<UsagePage />);
    await waitFor(async () =>
      expect(await screen.findByTestId("chart-error")).toBeVisible(),
    );
  });

  it("displays the invoices table for direct-billed organizations", async () => {
    server.use(buildDailyCostResponse());
    renderComponent(<UsagePage />);
    await waitFor(async () =>
      expect(await screen.findByTestId("invoice-table")).toBeVisible(),
    );
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
      buildDailyCostResponse(),
    );
    renderComponent(<UsagePage />);

    await waitFor(async () =>
      expect(await screen.findByTestId("aws-marketplace-banner")).toBeVisible(),
    );
  });

  it("correctly slices the right number of records for the time range when the range is equal to the number of days", () => {
    const dailyCosts: DailyCosts["daily"] = [
      {
        startDate: "2024-01-13T00:00:00Z",
        endDate: "2024-01-14T00:00:00Z",
        costs: {
          compute: { prices: [], subtotal: "0.00", total: "0.00" },
          storage: { prices: [], subtotal: "0.00", total: "0.00" },
        },
        total: "0.00",
        subtotal: "0.00",
      },
      {
        startDate: "2024-01-14T00:00:00Z",
        endDate: "2024-01-14T12:00:00Z",
        costs: {
          compute: { prices: [], subtotal: "0.00", total: "0.00" },
          storage: { prices: [], subtotal: "0.00", total: "0.00" },
        },
        total: "0.00",
        subtotal: "0.00",
      },
      {
        startDate: "2024-01-14T12:00:00Z",
        endDate: "2024-01-15T00:00:00Z",
        costs: {
          compute: { prices: [], subtotal: "0.00", total: "0.00" },
          storage: { prices: [], subtotal: "0.00", total: "0.00" },
        },
        total: "0.00",
        subtotal: "0.00",
      },
      {
        startDate: "2024-01-15T00:00:00Z",
        endDate: "2024-01-16T00:00:00Z",
        costs: {
          compute: { prices: [], subtotal: "0.00", total: "0.00" },
          storage: { prices: [], subtotal: "0.00", total: "0.00" },
        },
        total: "0.00",
        subtotal: "0.00",
      },
    ];
    // Get 3 days of the time range
    const sliced = getTimeRangeSlice(dailyCosts, 3);
    assert(sliced !== null);
    expect(sliced).toHaveLength(4);
    expect(sliced[0].startDate).toEqual(dailyCosts[0].startDate);
    expect(sliced[3].startDate).toEqual(dailyCosts[3].startDate);
  });

  it("generates appropriate time components for a time range", () => {
    const [start, end] = getTimeRange(7);
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
