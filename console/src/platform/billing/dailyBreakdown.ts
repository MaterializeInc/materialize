// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  CostBreakdownAccount,
  CostBreakdownCluster,
  CostBreakdownDay,
} from "~/api/cloudGlobalApi";

/**
 * Names aren't available yet (SAS-141/142), so accounts are labelled by their
 * `external_customer_id` UUID. The full id is very wide for a chip or a table
 * cell, so callers show this short prefix and keep the whole id in a tooltip.
 */
export function shortAccountId(accountId: string): string {
  return `${accountId.slice(0, 8)}…`;
}

/** Sum a cluster's per-price amounts (dollar strings) into a single total. */
export function clusterTotal(amounts: { [priceId: string]: string }): number {
  return Object.values(amounts).reduce(
    (sum, amount) => sum + parseFloat(amount),
    0,
  );
}

/** Total cost of an account across all of its clusters. */
export function accountTotal(account: CostBreakdownAccount): number {
  return account.clusters.reduce(
    (sum, cluster) => sum + clusterTotal(cluster.amounts),
    0,
  );
}

/**
 * Uniquely identifies a cluster row within an account across days. Quoted
 * Materialize identifiers can contain arbitrary characters, so this encodes
 * the fields as a JSON array rather than joining with a delimiter that could
 * itself appear inside one of them.
 */
function clusterKey(cluster: CostBreakdownCluster): string {
  return JSON.stringify([
    cluster.environment_id,
    cluster.cluster_grouping_key,
    cluster.category,
    cluster.region,
  ]);
}

/**
 * Restrict each day's accounts/clusters to a single region, dropping any
 * account left with no clusters. `"all"` returns `days` unchanged.
 */
export function filterDaysByRegion(
  days: CostBreakdownDay[],
  region: "all" | string,
): CostBreakdownDay[] {
  if (region === "all") {
    return days;
  }
  return days.map((day) => ({
    ...day,
    accounts: day.accounts
      .map((account) => ({
        ...account,
        clusters: account.clusters.filter(
          (cluster) => cluster.region === region,
        ),
      }))
      .filter((account) => account.clusters.length > 0),
  }));
}

/**
 * Collapse the per-day buckets into the period aggregate: one entry per
 * account, whose clusters' per-price amounts are summed across every day in the
 * window. Per-day cost is additive (SAS-134), so this reproduces the period
 * total. Used for the per-account comparison table and the single-account
 * cluster drilldown.
 */
export function aggregateDays(days: CostBreakdownDay[]): {
  accounts: CostBreakdownAccount[];
} {
  const accounts = new Map<string, Map<string, CostBreakdownCluster>>();
  for (const day of days) {
    for (const account of day.accounts) {
      let clusters = accounts.get(account.external_customer_id);
      if (!clusters) {
        clusters = new Map();
        accounts.set(account.external_customer_id, clusters);
      }
      for (const cluster of account.clusters) {
        const key = clusterKey(cluster);
        const existing = clusters.get(key);
        if (!existing) {
          clusters.set(key, { ...cluster, amounts: { ...cluster.amounts } });
          continue;
        }
        for (const [priceId, amount] of Object.entries(cluster.amounts)) {
          const prev = parseFloat(existing.amounts[priceId] ?? "0");
          existing.amounts[priceId] = (prev + parseFloat(amount)).toString();
        }
      }
    }
  }
  return {
    accounts: Array.from(accounts.entries()).map(
      ([external_customer_id, clusters]) => ({
        external_customer_id,
        clusters: Array.from(clusters.values()),
      }),
    ),
  };
}

/**
 * Per-account total cost for each day in the window, in `days` order — the
 * series behind the stacked-by-account chart and per-account trend sparklines.
 * Every account that appears on any day gets a full-length series, zero-filled
 * for days it had no usage.
 */
export function accountDailyTotals(
  days: CostBreakdownDay[],
): Map<string, number[]> {
  const series = new Map<string, number[]>();
  days.forEach((day, dayIndex) => {
    for (const account of day.accounts) {
      let daily = series.get(account.external_customer_id);
      if (!daily) {
        daily = new Array(days.length).fill(0);
        series.set(account.external_customer_id, daily);
      }
      daily[dayIndex] = accountTotal(account);
    }
  });
  return series;
}

/**
 * Account ids ordered by descending period total (ties broken by id for a
 * stable order). Drives the stacked-chart layer order, the legend, and the
 * comparison table's row order so the biggest spender is on top.
 */
export function accountIdsByTotal(days: CostBreakdownDay[]): string[] {
  const totals = new Map<string, number>();
  for (const [id, series] of accountDailyTotals(days)) {
    totals.set(
      id,
      series.reduce((sum, amount) => sum + amount, 0),
    );
  }
  return Array.from(totals.keys()).sort((a, b) => {
    const diff = (totals.get(b) ?? 0) - (totals.get(a) ?? 0);
    return diff !== 0 ? diff : a.localeCompare(b);
  });
}

/** One stacked-chart row per day: the day plus each account's total for it. */
export type StackedDailyRow = { startDate: string } & {
  [accountId: string]: number | string;
};

/**
 * Reshape the per-account daily series into one row per day keyed by account
 * id — the row shape visx `BarStack` consumes (`keys` = `accountIds`). Every
 * account is present on every row (zero-filled), so stacks line up across days.
 */
export function stackedDailyRows(
  days: CostBreakdownDay[],
  accountIds: string[],
): StackedDailyRow[] {
  const series = accountDailyTotals(days);
  return days.map((day, dayIndex) => {
    const row: StackedDailyRow = { startDate: day.startDate };
    for (const id of accountIds) {
      row[id] = series.get(id)?.[dayIndex] ?? 0;
    }
    return row;
  });
}
