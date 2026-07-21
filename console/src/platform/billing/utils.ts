// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { UTCDate } from "@date-fns/utc";
import { addMonths, parseISO, startOfMonth, subDays } from "date-fns";

import { DailyCosts, Organization } from "~/api/cloudGlobalApi";
import { components } from "~/api/schemas/global-api";
import { CloudRegion } from "~/store/cloudRegions";

import { ROLLING_AVG_TIME_RANGE_LOOKBACK_DAYS } from "./constants";
import { RegionGroupedSummary } from "./types";

export function summarizePlanCosts(
  dailyCosts: DailyCosts["daily"] | null,
  timeSpan: number,
  availableRegions: Map<string, CloudRegion>,
) {
  // Since we only return non-zero buckets from the API endpoint, ensure we're
  // always showing known-available regions.
  const defaultRegionEntries: Array<[string, number]> = Array.from(
    availableRegions.keys(),
  ).map((regionId) => [regionId, 0]);
  const spanSummary: RegionGroupedSummary = {
    total: 0,
    regions: new Map(defaultRegionEntries),
  };
  const last30Summary: RegionGroupedSummary = {
    total: 0,
    regions: new Map(defaultRegionEntries),
  };
  if (dailyCosts === null) return { spanSummary, last30Summary };
  const lastStartDate = parseISO(dailyCosts[dailyCosts.length - 1].startDate);
  // If a plan has changed mid-day, the start date of the last slice may not
  // start at midnight. Align the time component to the start of the day.
  lastStartDate.setUTCHours(0, 0, 0, 0);
  for (const day of dailyCosts) {
    const startDate = parseISO(day.startDate);
    const isWithin30Days =
      subDays(
        lastStartDate,
        ROLLING_AVG_TIME_RANGE_LOOKBACK_DAYS - 1,
      ).getTime() <= startDate.getTime();
    // Whether or not the day being computed is within the filtered window. We
    // need to check this because the minimum queried span is 30 days, but the
    // client may filter down to as low as 7 days.
    const isWithinTimeRange =
      subDays(lastStartDate, timeSpan - 1).getTime() <= startDate.getTime();
    for (const costCategory of Object.values(day.costs)) {
      for (const { regionId, subtotal } of costCategory.prices) {
        if (regionId === "global") {
          // Unattributable to a region.
          continue;
        }
        if (isWithinTimeRange) {
          spanSummary.regions.set(
            regionId,
            // Don't assume the region entry exists (a customer could have lost
            // access to a region)
            (spanSummary.regions.get(regionId) ?? 0) + parseFloat(subtotal),
          );
          spanSummary.total += parseFloat(subtotal);
        }
        if (isWithin30Days) {
          last30Summary.regions.set(
            regionId,
            // Don't assume the region entry exists (a customer could have lost
            // access to a region)
            (last30Summary.regions.get(regionId) ?? 0) + parseFloat(subtotal),
          );
          last30Summary.total += parseFloat(subtotal);
        }
      }
    }
  }
  return { spanSummary, last30Summary };
}

export function getIsUpgradedPlan(
  planType: components["schemas"]["PlanType"] | undefined,
): planType is Exclude<components["schemas"]["PlanType"], "evaluation"> {
  return planType !== "evaluation";
}

export function calculateNextOnDemandPaymentDate() {
  const nextPaymentDate = startOfMonth(addMonths(new UTCDate(), 1));
  return nextPaymentDate;
}

export function getIsTrialExpired(organization: Organization): boolean {
  return (
    organization.subscription?.type === "evaluation" && organization.blocked
  );
}

export function getIsUpgrading(organization: Organization): boolean {
  // We assume that Trial/evaluation accounts should never have a payment method, otherwise
  // the account would've already been upgraded to an on-demand account via our sync-server.
  // This case only occurs when the payment method has been added to Stripe,
  // but the account is not yet upgraded to an on-demand account.
  return (
    organization.subscription?.type === "evaluation" &&
    (organization.paymentMethods?.length ?? 0) > 0
  );
}
