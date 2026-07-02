// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CostBreakdownDay } from "~/api/cloudGlobalApi";

import {
  accountDailyTotals,
  accountIdsByTotal,
  accountTotal,
  aggregateDays,
  stackedDailyRows,
} from "./dailyBreakdown";

function cluster(
  overrides: Partial<{
    environment_id: string;
    cluster_grouping_key: string;
    category: string;
    region: string;
    amounts: { [k: string]: string };
  }> = {},
) {
  return {
    environment_id: "environment-acct-0",
    cluster_grouping_key: "quickstart.r1",
    category: "",
    region: "aws/us-east-1",
    amounts: { "price-compute": "10.00" },
    ...overrides,
  };
}

describe("aggregateDays", () => {
  it("sums a cluster's amounts across days into the period total", () => {
    const days: CostBreakdownDay[] = [
      {
        startDate: "2026-06-01T00:00:00Z",
        endDate: "2026-06-02T00:00:00Z",
        accounts: [
          {
            external_customer_id: "acct",
            clusters: [cluster({ amounts: { "price-compute": "10.00" } })],
          },
        ],
      },
      {
        startDate: "2026-06-02T00:00:00Z",
        endDate: "2026-06-03T00:00:00Z",
        accounts: [
          {
            external_customer_id: "acct",
            clusters: [cluster({ amounts: { "price-compute": "4.50" } })],
          },
        ],
      },
    ];

    const { accounts } = aggregateDays(days);
    expect(accounts).toHaveLength(1);
    expect(accounts[0].clusters).toHaveLength(1);
    expect(accounts[0].clusters[0].amounts["price-compute"]).toBe("14.5");
    expect(accountTotal(accounts[0])).toBeCloseTo(14.5);
  });

  it("keeps storage and egress (same empty cluster key) as distinct rows by category", () => {
    const clusters = [
      cluster({
        cluster_grouping_key: "",
        category: "Storage",
        amounts: { "price-storage": "0.50" },
      }),
      cluster({
        cluster_grouping_key: "",
        category: "Egress",
        amounts: { "price-egress": "0.25" },
      }),
    ];
    const days: CostBreakdownDay[] = [
      {
        startDate: "2026-06-01T00:00:00Z",
        endDate: "2026-06-02T00:00:00Z",
        accounts: [{ external_customer_id: "acct", clusters }],
      },
    ];

    const { accounts } = aggregateDays(days);
    const categories = accounts[0].clusters.map((c) => c.category).sort();
    expect(categories).toEqual(["Egress", "Storage"]);
  });
});

describe("accountDailyTotals", () => {
  it("returns a zero-filled per-day series for every account", () => {
    const days: CostBreakdownDay[] = [
      {
        startDate: "2026-06-01T00:00:00Z",
        endDate: "2026-06-02T00:00:00Z",
        accounts: [
          {
            external_customer_id: "a",
            clusters: [cluster({ amounts: { c: "3.00" } })],
          },
        ],
      },
      {
        // "b" first appears on day 2; "a" has no usage this day.
        startDate: "2026-06-02T00:00:00Z",
        endDate: "2026-06-03T00:00:00Z",
        accounts: [
          {
            external_customer_id: "b",
            clusters: [cluster({ amounts: { c: "7.00" } })],
          },
        ],
      },
    ];

    const series = accountDailyTotals(days);
    expect(series.get("a")).toEqual([3, 0]);
    expect(series.get("b")).toEqual([0, 7]);
  });
});

describe("accountIdsByTotal / stackedDailyRows", () => {
  const days: CostBreakdownDay[] = [
    {
      startDate: "2026-06-01T00:00:00Z",
      endDate: "2026-06-02T00:00:00Z",
      accounts: [
        {
          external_customer_id: "small",
          clusters: [cluster({ amounts: { c: "1.00" } })],
        },
        {
          external_customer_id: "big",
          clusters: [cluster({ amounts: { c: "5.00" } })],
        },
      ],
    },
    {
      startDate: "2026-06-02T00:00:00Z",
      endDate: "2026-06-03T00:00:00Z",
      accounts: [
        {
          external_customer_id: "big",
          clusters: [cluster({ amounts: { c: "5.00" } })],
        },
      ],
    },
  ];

  it("orders account ids by descending period total", () => {
    expect(accountIdsByTotal(days)).toEqual(["big", "small"]);
  });

  it("builds one zero-filled row per day, keyed by every account", () => {
    const ids = accountIdsByTotal(days);
    expect(stackedDailyRows(days, ids)).toEqual([
      { startDate: "2026-06-01T00:00:00Z", big: 5, small: 1 },
      { startDate: "2026-06-02T00:00:00Z", big: 5, small: 0 },
    ]);
  });
});
