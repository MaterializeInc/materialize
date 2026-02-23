// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import FastPathClustersExtantInsight, {
  VERSIONED_ID as fastPathClustersExtantVersionedId,
} from "./FastPathClustersExtantInsight";
import NonHydratedDependenciesInsight, {
  VERSIONED_ID as nonHydratedDependenciesVersionedId,
} from "./NonHydratedDependenciesInsight";
import NonRunningSourcesInsight, {
  VERSIONED_ID as nonRunningSourcesVersionedId,
} from "./NonRunningSourcesInsight";
import OutdatedDependenciesInsights, {
  VERSIONED_ID as outdatedDependenciesVersionedId,
} from "./OutdatedDependenciesInsight";
import { PlanInsights } from "./PlanInsightsNotice";

function hasNonHydratedDependencies(planInsights: PlanInsights) {
  return Object.values(planInsights.blockedDependencies).some(
    ({ hydrated }) => hydrated === false,
  );
}

function areFastPathClustersExtant(planInsights: PlanInsights) {
  return Object.keys(planInsights.fastPathClusters).length > 0;
}

function hasNonRunningSources(planInsights: PlanInsights) {
  return Object.keys(planInsights.nonRunningSources).length > 0;
}

function hasOutdatedDependencies(planInsights: PlanInsights) {
  return Object.values(planInsights.blockedDependencies).some(
    ({ isOutdated }) => !!isOutdated,
  );
}

// This is a list of insights that can be rendered in the drawer. Each insight
// is ordered by usefulness
export const INSIGHTS_LIST: Array<{
  shouldRender: (planInsights: PlanInsights) => boolean;
  component: React.FC<{ planInsights: PlanInsights }>;
  versionedId: string;
}> = [
  {
    shouldRender: hasNonRunningSources,
    component: NonRunningSourcesInsight,
    versionedId: nonRunningSourcesVersionedId,
  },
  {
    shouldRender: hasNonHydratedDependencies,
    component: NonHydratedDependenciesInsight,
    versionedId: nonHydratedDependenciesVersionedId,
  },
  {
    shouldRender: hasOutdatedDependencies,
    component: OutdatedDependenciesInsights,
    versionedId: outdatedDependenciesVersionedId,
  },
  {
    shouldRender: areFastPathClustersExtant,
    component: FastPathClustersExtantInsight,
    versionedId: fastPathClustersExtantVersionedId,
  },
];

export function countVisiblePlanInsights(planInsights: PlanInsights) {
  return INSIGHTS_LIST.reduce(
    (accum, { shouldRender }) => accum + Number(shouldRender(planInsights)),
    0,
  );
}

export function planInsightsToInstrumentationIds(planInsights: PlanInsights) {
  return INSIGHTS_LIST.reduce((accum, { shouldRender, versionedId }) => {
    if (shouldRender(planInsights)) {
      accum.push(versionedId);
    }
    return accum;
  }, [] as string[]);
}

/**
 * TODO: (#3400) Remove once all subsources are tables
 * For objects that are sources and subsources, we want to label them as just tables
 */
export function determineObjectType({
  objectType,
  sourceType,
}: {
  objectType: string;
  sourceType: string | null;
}) {
  return sourceType === "subsource" ? sourceType : objectType;
}
