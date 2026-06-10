// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { render, screen } from "@testing-library/react";
import { TooltipInPortalProps } from "@visx/tooltip/lib/hooks/useTooltipInPortal";
import React, { ReactElement } from "react";

import { DailyCosts } from "~/api/cloudGlobalApi";
import { createProviderWrapper } from "~/test/utils";

import {
  DailyUsageChartTooltip,
  LegendEntry,
  TooltipData,
} from "./DailyUsageChart";
import { aggregateByDay } from "./utils";

const Wrapper = await createProviderWrapper();

const renderComponent = (element: ReactElement) => {
  return render(<Wrapper>{element}</Wrapper>);
};

const legendData: LegendEntry[] = [
  { key: "compute", label: "Compute", color: "purple" },
  { key: "storage", label: "Storage", color: "gray" },
];

const mockData: TooltipData = {
  bar: {
    day: {
      startDate: "2024-01-13T00:00:00Z",
      endDate: "2024-01-14T00:00:00Z",
      subtotal: "1.23",
      total: "1.23",
      costs: {
        compute: {
          subtotal: "1.00",
          total: "1.00",
          prices: [
            {
              subtotal: "0.50",
              regionId: "aws/us-east-1",
              unitAmount: "1.00",
              replicaSize: "xsmall",
            },
            {
              subtotal: "0.50",
              regionId: "aws/eu-west-1",
              unitAmount: "1.00",
              replicaSize: "small",
            },
          ],
        },
        storage: {
          subtotal: "1.23",
          total: "1.23",
          prices: [
            { subtotal: "1.00", regionId: "aws/us-east-1", unitAmount: "1.00" },
            { subtotal: "0.23", regionId: "aws/eu-west-1", unitAmount: "1.00" },
          ],
        },
      },
    },
    index: 0,
    height: 0,
    width: 0,
    x: 0,
    y: 0,
    color: "gray",
  },
  hoveredIndex: 0,
};

const MockTooltip = ({
  children,
  ...props
}: React.PropsWithChildren<TooltipInPortalProps>) => {
  return <div {...props}>{children}</div>;
};

describe("UsageChart", () => {
  it("tooltip displays two content columns when not filtering by region", async () => {
    renderComponent(
      <DailyUsageChartTooltip
        top={0}
        left={0}
        legendData={legendData}
        tooltipData={mockData}
        availableRegions={["aws/us-east-1", "aws/eu-west-1"]}
        allRegions={true}
        component={MockTooltip}
      />,
    );
    const startDate = await screen.findByTestId("chart-tooltip-start-date");
    expect(startDate).toHaveTextContent("1/13/2024");
    const table = await screen.findByTestId("chart-tooltip-table");
    const headerCells = Array.from(table.querySelectorAll("th"));
    expect(headerCells).toHaveLength(3);
    expect(headerCells[1]).toHaveTextContent("aws/us-east-1");
    expect(headerCells[2]).toHaveTextContent("aws/eu-west-1");
    const tableCells = Array.from(table.querySelectorAll("td"));
    expect(tableCells).toHaveLength(6);
    expect(
      parseFloat(tableCells[1].textContent?.replace("$", "") ?? "0") +
        parseFloat(tableCells[2].textContent?.replace("$", "") ?? "0"),
    ).toEqual(parseFloat(mockData.bar.day.costs.compute.subtotal));
    expect(
      parseFloat(tableCells[4].textContent?.replace("$", "") ?? "0") +
        parseFloat(tableCells[5].textContent?.replace("$", "") ?? "0"),
    ).toEqual(parseFloat(mockData.bar.day.costs.storage.subtotal));
  });

  it("tooltip displays a single content column when filtering by region", async () => {
    renderComponent(
      <DailyUsageChartTooltip
        top={0}
        left={0}
        legendData={legendData}
        tooltipData={mockData}
        availableRegions={["aws/us-east-1"]}
        allRegions={false}
        component={MockTooltip}
      />,
    );
    const table = await screen.findByTestId("chart-tooltip-table");
    const headerCells = Array.from(table.querySelectorAll("th"));
    expect(headerCells.length).toEqual(2);
    expect(headerCells[1]).toBeEmptyDOMElement();
  });

  it("daily costs are aggregated by day", () => {
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
    const aggregated = aggregateByDay(costData);
    expect(aggregated).toHaveLength(2);

    const day1 = aggregated[0];
    expect(day1.startDate).toEqual("2024-01-12T00:00:00Z");
    expect(day1.endDate).toEqual("2024-01-13T00:00:00Z");
    expect(day1.total).toEqual("1.00");
    expect(day1.subtotal).toEqual("1.00");
    expect(day1.costs.compute.subtotal).toEqual("0.50");
    expect(day1.costs.compute.total).toEqual("0.50");
    expect(day1.costs.compute.prices).toEqual([
      {
        regionId: "aws/us-east-1",
        replicaSize: "large",
        unitAmount: "1.20",
        subtotal: "0.50",
      },
    ]);
    expect(day1.costs.storage.subtotal).toEqual("0.50");
    expect(day1.costs.storage.total).toEqual("0.50");
    expect(day1.costs.storage.prices).toEqual([
      {
        regionId: "aws/us-east-1",
        unitAmount: "0.000081",
        subtotal: "0.50",
      },
    ]);

    const day2 = aggregated[1];
    expect(day2.startDate).toEqual("2024-01-13T00:00:00Z");
    expect(day2.endDate).toEqual("2024-01-14T00:00:00Z");
    expect(parseFloat(day2.total)).toEqual(3);
    expect(parseFloat(day2.subtotal)).toEqual(3);
    expect(parseFloat(day2.costs.compute.subtotal)).toEqual(2);
    expect(parseFloat(day2.costs.compute.total)).toEqual(2);
    expect(day2.costs.compute.prices).toEqual([
      {
        regionId: "aws/us-east-1",
        replicaSize: "large",
        unitAmount: "-1",
        subtotal: "2",
      },
    ]);
    expect(parseFloat(day2.costs.storage.subtotal)).toEqual(1);
    expect(parseFloat(day2.costs.storage.total)).toEqual(1);
    expect(day2.costs.storage.prices).toEqual([
      {
        regionId: "aws/us-east-1",
        unitAmount: "0.000081",
        subtotal: "1",
      },
    ]);
  });
});
