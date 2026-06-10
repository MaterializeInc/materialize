// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen } from "@testing-library/react";
import { parseISO } from "date-fns";
import React, { ReactElement } from "react";

import { DailyCosts } from "~/api/cloudGlobalApi";
import {
  buildCloudRegionsReponse,
  buildInvoicesResponse,
  generateDailyCostResponsePayload,
} from "~/api/mocks/cloudGlobalApiHandlers";
import server from "~/api/mocks/server";
import {
  createProviderWrapper,
  healthyEnvironment,
  setFakeEnvironment,
} from "~/test/utils";
import { assert } from "~/util";

import SpendBreakdown from "./SpendBreakdown";
import { summarizeResourceCosts } from "./utils";

const Wrapper = await createProviderWrapper({
  initializeState: ({ set }) =>
    setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
});

const renderComponent = (element: ReactElement) => {
  return render(<Wrapper>{element}</Wrapper>);
};

describe("SpendBreakdown", () => {
  beforeEach(() => {
    server.use(buildCloudRegionsReponse(), buildInvoicesResponse());
  });

  it("renders successfully for all regions", async () => {
    const startDate = parseISO("2024-01-01T00:00:00.000+00:00");
    const endDate = parseISO("2024-01-07T00:00:00.000+00:00");
    const payload = generateDailyCostResponsePayload(startDate, endDate);
    renderComponent(
      <SpendBreakdown region="all" dailyCosts={payload.daily} totalDays={7} />,
    );
    const planDetails = await screen.findByTestId("spend-breakdown-range");
    expect(planDetails).toHaveTextContent(
      "Spend between 01-01-24 and 01-06-24",
    );
    const computeTable = screen.getByTestId("spend-breakdown-compute-group");
    expect(computeTable).toHaveTextContent("aws/us-east-1");
    expect(computeTable).toHaveTextContent("aws/eu-west-1");
    const storageTable = screen.getByTestId("spend-breakdown-storage-group");
    expect(storageTable).toHaveTextContent("aws/us-east-1");
    expect(storageTable).toHaveTextContent("aws/eu-west-1");
  });

  it("hides the details when data is being loaded", async () => {
    renderComponent(
      <SpendBreakdown region="all" dailyCosts={null} totalDays={7} />,
    );
    const planDetails = await screen.findByTestId("spend-breakdown-range");
    expect(planDetails).toHaveTextContent("Spend between");
    const computeTable = screen.getByTestId("spend-breakdown-compute-group");
    expect(computeTable).not.toHaveTextContent("aws/us-east-1");
    expect(computeTable).not.toHaveTextContent("aws/eu-west-1");
    const storageTable = screen.getByTestId("spend-breakdown-storage-group");
    expect(storageTable).not.toHaveTextContent("aws/us-east-1");
    expect(storageTable).not.toHaveTextContent("aws/eu-west-1");
  });

  it("only displays filtered regions", async () => {
    const startDate = parseISO("2024-01-01T00:00:00.000+00:00");
    const endDate = parseISO("2024-01-07T00:00:00.000+00:00");
    const payload = generateDailyCostResponsePayload(startDate, endDate);
    renderComponent(
      <SpendBreakdown
        region="aws/us-east-1"
        dailyCosts={payload.daily}
        totalDays={7}
      />,
    );
    const computeTable = screen.getByTestId("spend-breakdown-compute-group");
    expect(computeTable).not.toHaveTextContent("aws/us-east-1");
    expect(computeTable).not.toHaveTextContent("aws/eu-west-1");
    expect(computeTable).toHaveTextContent("credits");
    const storageTable = screen.getByTestId("spend-breakdown-storage-group");
    expect(storageTable).not.toHaveTextContent("aws/us-east-1");
    expect(storageTable).not.toHaveTextContent("aws/eu-west-1");
  });

  it("correctly aggregates midday plan changes", () => {
    const costData: DailyCosts["daily"] = [
      {
        startDate: "2024-01-12T00:00:00Z",
        endDate: "2024-01-13T12:00:00Z",
        subtotal: "1.00",
        total: "1.00",
        costs: {
          compute: {
            subtotal: "0.50",
            total: "0.50",
            prices: [
              {
                regionId: "aws/us-east-1",
                replicaSize: "large",
                unitAmount: "1.20",
                subtotal: "0.50",
              },
            ],
          },
          storage: {
            subtotal: "0.50",
            total: "0.50",
            prices: [
              {
                regionId: "aws/us-east-1",
                unitAmount: "0.000081",
                subtotal: "0.50",
              },
            ],
          },
        },
      },
      {
        startDate: "2024-01-13T00:00:00Z",
        endDate: "2024-01-13T12:00:00Z",
        subtotal: "1.00",
        total: "1.00",
        costs: {
          compute: {
            subtotal: "0.50",
            total: "0.50",
            prices: [
              {
                regionId: "aws/us-east-1",
                replicaSize: "large",
                unitAmount: "1.20",
                subtotal: "0.50",
              },
            ],
          },
          storage: {
            subtotal: "0.50",
            total: "0.50",
            prices: [
              {
                regionId: "aws/us-east-1",
                unitAmount: "0.000081",
                subtotal: "0.50",
              },
            ],
          },
        },
      },
      {
        startDate: "2024-01-13T12:00:00Z",
        endDate: "2024-01-14T00:00:00Z",
        subtotal: "2.00",
        total: "2.00",
        costs: {
          compute: {
            subtotal: "1.50",
            total: "1.50",
            prices: [
              {
                regionId: "aws/us-east-1",
                replicaSize: "large",
                unitAmount: "1.50",
                subtotal: "1.50",
              },
            ],
          },
          storage: {
            subtotal: "0.50",
            total: "0.50",
            prices: [
              {
                regionId: "aws/us-east-1",
                unitAmount: "0.000081",
                subtotal: "0.50",
              },
            ],
          },
        },
      },
    ];
    const summarized = summarizeResourceCosts(costData, 2);
    expect(summarized).toHaveLength(1);
    const region = summarized.get("aws/us-east-1");
    expect(region).toBeDefined();
    assert(region);
    expect(region.compute.total.usagePoints).toHaveLength(2);
    expect(region.compute.total.totalCost).toEqual(2.5);
    expect(region.storage.total.usagePoints).toHaveLength(2);
    expect(region.storage.total.totalCost).toEqual(1.5);
  });
});
