// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { UTCDate } from "@date-fns/utc";
import { addDays, addMonths, parseISO, startOfMonth, subDays } from "date-fns";

import {
  DailyCostKey,
  DailyCosts,
  Organization,
  Prices,
} from "~/api/cloudGlobalApi";
import { components } from "~/api/schemas/global-api";
import { CloudRegion } from "~/store/cloudRegions";
import { assert } from "~/util";
import { formatDateInUtc, formatUtcIso } from "~/utils/dateFormat";

import {
  costUnits,
  replicaSorts,
  ROLLING_AVG_TIME_RANGE_LOOKBACK_DAYS,
} from "./constants";
import { RegionGroupedSummary, RegionResourceBreakdown } from "./types";

export function aggregateByDay(data: DailyCosts["daily"]) {
  const days = new Map<string, DailyCosts["daily"][0]>();
  for (const slice of data) {
    const startDate = parseISO(slice.startDate);
    const formattedDate = formatDateInUtc(startDate);
    let day = days.get(formattedDate);
    if (!day) {
      startDate.setUTCHours(0, 0, 0, 0);
      day = {
        costs: structuredClone(slice.costs),
        startDate: formatUtcIso(startDate),
        endDate: formatUtcIso(addDays(startDate, 1)),
        subtotal: slice.subtotal,
        total: slice.total,
      };
    } else {
      day = rollupSlice(day, slice);
    }
    days.set(formattedDate, day);
  }
  return Array.from(days.values());
}

function rollupSlice(
  day: DailyCosts["daily"][0],
  slice: DailyCosts["daily"][0],
): DailyCosts["daily"][0] {
  day.subtotal = sumStringFloats(day.subtotal, slice.subtotal);
  day.total = sumStringFloats(day.total, slice.total);
  for (const costKey of ["storage", "compute"] as const) {
    day.costs[costKey].subtotal = sumStringFloats(
      day.costs[costKey].subtotal,
      slice.costs[costKey].subtotal,
    );
    day.costs[costKey].total = sumStringFloats(
      day.costs[costKey].total,
      slice.costs[costKey].total,
    );
    for (const slicePrice of slice.costs[costKey].prices) {
      const dayPrice = day.costs[costKey].prices.find((p) => {
        return (
          p.regionId === slicePrice.regionId &&
          // compare replicaIds for compute prices, otherwise just regionId
          ("replicaId" in slicePrice && "replicaId" in p
            ? slicePrice.replicaId === p.replicaId
            : true)
        );
      });
      if (dayPrice) {
        dayPrice.subtotal = sumStringFloats(
          dayPrice.subtotal,
          slicePrice.subtotal,
        );
        // If the rate is changed, poison the unit amount so we don't attempt
        // to do any calculations based on it. if we end up needing this we can
        // update the data structure.
        dayPrice.unitAmount =
          dayPrice.unitAmount === slicePrice.unitAmount
            ? dayPrice.unitAmount
            : "-1";
      } else {
        (day.costs[costKey].prices as Prices[]).push(
          structuredClone(slicePrice),
        );
      }
    }
  }
  return day;
}

function sumStringFloats(left: string, right: string): string {
  return (parseFloat(left) + parseFloat(right)).toString();
}

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

export function getTimeRangeSlice(
  dailyCosts: DailyCosts["daily"] | null,
  timeRangeFilter: number,
): DailyCosts["daily"] | null {
  if (dailyCosts === null || dailyCosts.length <= timeRangeFilter) {
    return dailyCosts;
  }
  const lastStartDate = parseISO(dailyCosts[dailyCosts.length - 1].startDate);
  // Determine the starting date for the time range (subtract one because
  // we're starting from the beginning of the last day).
  const firstStartDate = subDays(lastStartDate, timeRangeFilter - 1);
  let startOffset = -1 * timeRangeFilter;
  // Seed the list with the known-minimum number of records from the tail.
  const slices = dailyCosts.slice(startOffset);
  startOffset--;
  // ... then walk backwards from where we left off, prepending all records that
  // fall on or after the start of the time range. Since we're counting back
  // from the end of the Array, this is an inclusive guard (otherwise we'd omit
  // the last element when calling `.at(-n)`).
  while (Math.abs(startOffset) <= dailyCosts.length) {
    const slice = dailyCosts.at(startOffset);
    if (!slice) break;
    const sliceStart = parseISO(slice.startDate);
    if (firstStartDate.getTime() <= sliceStart.getTime()) {
      slices.unshift(slice);
    } else {
      break;
    }
    startOffset--;
  }
  return slices;
}

export function summarizeResourceCosts(
  dailyCosts: DailyCosts["daily"],
  totalDays: number,
) {
  const breakdown: RegionResourceBreakdown = new Map();
  let previousDate: string | null = null;
  let offset = 0;
  for (const day of dailyCosts) {
    // If a plan is changed mid-day, we may have multiple slices per day. Only
    // advance the the index when a change occurs.
    const startDate = formatDateInUtc(parseISO(day.startDate));
    if (previousDate === null) {
      previousDate = startDate;
    }
    // If this slice starts on a new day, advance the day offset.
    if (previousDate !== startDate) {
      previousDate = startDate;
      offset++;
    }
    summarizeDayCost(breakdown, day.costs, offset, "compute", totalDays);
    summarizeDayCost(breakdown, day.costs, offset, "storage", totalDays);
  }
  return breakdown;
}

function summarizeDayCost(
  breakdown: RegionResourceBreakdown,
  costs: DailyCosts["daily"][0]["costs"],
  dayIx: number,
  costKey: DailyCostKey,
  totalDays: number,
) {
  for (const priceBlock of costs[costKey].prices) {
    if (!breakdown.has(priceBlock.regionId)) {
      breakdown.set(priceBlock.regionId, getEmptyBreakdown(totalDays));
    }
    const regionSummary = breakdown.get(priceBlock.regionId);
    assert(regionSummary);
    const costSummary = regionSummary[costKey];
    const priceCost = parseFloat(priceBlock.subtotal);
    const creditsCost = priceCost / parseFloat(priceBlock.unitAmount);
    // Add this cost value (e.g., storage or compute) to the region-wide total for the cost category
    costSummary.total.totalCost += priceCost;
    costSummary.total.usageValue += creditsCost;
    costSummary.total.usagePoints[dayIx] += priceCost;
    const resourceType =
      "replicaSize" in priceBlock ? priceBlock.replicaSize : "Storage";
    if (!costSummary.resources.has(resourceType)) {
      costSummary.resources.set(resourceType, {
        usagePoints: Array(costSummary.total.usagePoints.length).fill(0),
        totalCost: 0,
        sort: getReplicaSort(resourceType),
        usageUnits: costUnits[costKey],
        usageValue: 0,
        rate: "0.00",
      });
    }
    // Add this cost value (e.g., storage or compute) to the region-wide total
    // for the resource (e.g., `xsmall` replica)
    const resourceSummary = costSummary.resources.get(resourceType);
    assert(resourceSummary);
    resourceSummary.usageValue += creditsCost;
    resourceSummary.totalCost += priceCost;
    resourceSummary.usagePoints[dayIx] += priceCost;
    resourceSummary.rate = priceBlock.unitAmount;
  }
}

function getReplicaSort(replicaName: string): number {
  if (replicaName.includes("cc")) {
    const size = replicaName.split("cc", 1)[0];
    return parseInt(size);
  } else if (replicaName.includes("C")) {
    const size = replicaName.split("C", 1)[0];
    return parseInt(size) * 100;
  }
  return replicaSorts.get(replicaName) ?? 0;
}

function getEmptyBreakdown(days: number) {
  return {
    compute: {
      total: {
        usageValue: 0,
        usageUnits: costUnits.compute,
        usagePoints: Array(days).fill(0),
        totalCost: 0,
      },
      resources: new Map(),
    },
    storage: {
      total: {
        usageValue: 0,
        usageUnits: costUnits.storage,
        usagePoints: Array(days).fill(0),
        totalCost: 0,
      },
      resources: new Map(),
    },
  };
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
