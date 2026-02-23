// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen } from "@testing-library/react";
import React, { ReactElement } from "react";

import { DailyCosts } from "~/api/cloudGlobalApi";
import {
  buildCloudRegionsReponse,
  buildCreditsResponse,
  generateDailyCostResponsePayload,
} from "~/api/mocks/cloudGlobalApiHandlers";
import server from "~/api/mocks/server";
import {
  createProviderWrapper,
  healthyEnvironment,
  setFakeEnvironment,
} from "~/test/utils";

import { UpgradedPlanDetails } from "./PlanDetails";
import { getTimeRange } from "./queries";
import { summarizePlanCosts } from "./utils";

const Wrapper = await createProviderWrapper({
  initializeState: ({ set }) =>
    setFakeEnvironment(set, "aws/us-east-1", healthyEnvironment),
});

const renderComponent = (element: ReactElement) => {
  return render(<Wrapper>{element}</Wrapper>);
};

describe("UpgradedPlanDetails", () => {
  beforeEach(() => {
    server.use(buildCloudRegionsReponse(), buildCreditsResponse());
  });

  it("renders successfully for all regions", async () => {
    const [startDate, endDate] = getTimeRange(7);
    const payload = generateDailyCostResponsePayload(startDate, endDate);
    renderComponent(
      <UpgradedPlanDetails region="all" dailyCosts={payload} timeSpan={7} />,
    );

    expect(await screen.findByText("Capacity")).toBeVisible();
    expect(await screen.findByText("Total balance")).toBeVisible();
  });

  it("hides region breakdown when filtering regions", async () => {
    const [startDate, endDate] = getTimeRange(7);
    const payload = generateDailyCostResponsePayload(startDate, endDate);
    renderComponent(
      <UpgradedPlanDetails
        region="aws/us-east-1"
        dailyCosts={payload}
        timeSpan={7}
      />,
    );
    const planDetails = await screen.findByTestId("plan-details");
    expect(planDetails.textContent).toContain("Capacity");
    expect(planDetails.textContent).toContain("Total balance");
    expect(screen.queryAllByText("aws/us-east-1")[0]).not.toBeVisible();
  });

  it("correctly handles midday plan changes", () => {
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
    const { spanSummary, last30Summary } = summarizePlanCosts(
      costData,
      2,
      new Map([
        [
          "aws/us-east-1",
          { provider: "aws", region: "us-east-1", regionApiUrl: "example.com" },
        ],
      ]),
    );
    expect(spanSummary.total).toEqual(4);
    expect(last30Summary.total).toEqual(4);
  });
});
