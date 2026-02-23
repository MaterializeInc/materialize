// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom, useAtom } from "jotai";

import { appConfigAtom } from "~/config/store";
import { fetchCloudRegions } from "~/queries/cloudGlobalApi";
import storageAvailable from "~/utils/storageAvailable";

export type RegionId = string;

/** Represents a region of a cloud provider in which Materialize operators. */
export interface CloudRegion {
  /** The identifier of the cloud provider (e.g., `aws`). */
  provider: string;
  /** The region of the cloud provider (e.g., `us-east-1`). */
  region: string;
  /** The URL for the region api. */
  regionApiUrl: string;
}

export const SELECTED_REGION_KEY = "mz-selected-region";

/** Constructs a short, unique, human-readable identifier for a cloud region.
 *
 * Because cloud regions and environments are 1:1, this function is also usable
 * for constructing the ID for an environment.
 */
export const getRegionId = (region: CloudRegion): RegionId => {
  return `${region.provider}/${region.region}`;
};

export const getStylizedCloudRegionProvider = (region: CloudRegion): string => {
  switch (region.provider) {
    case "aws":
      return "AWS";
    default:
      return region.provider;
  }
};

export const fetchOrBuildCloudRegions = async (): Promise<CloudRegion[]> => {
  const body = await fetchCloudRegions();
  return body.data
    .map((r) => ({
      provider: r.cloudProvider,
      region: r.name,
      regionApiUrl: r.url,
    }))
    .sort((a, b) => b.region.localeCompare(a.region));
};

export const cloudRegionsSelector = atom(async (get) => {
  const appConfig = get(appConfigAtom);

  let regions = [];

  if (appConfig.mode === "cloud" && appConfig.cloudRegionsOverride !== null) {
    regions = appConfig.cloudRegionsOverride;
  } else if (appConfig.mode === "self-managed") {
    regions = appConfig.regionsStub;
  } else {
    regions = await fetchOrBuildCloudRegions();
  }

  return new Map(regions.map((r) => [getRegionId(r), r]));
});

/**
 * Returns the user's previously selected region, or the first region in the list.
 */
export const preferredCloudRegion = atom(async (get) => {
  if (!storageAvailable("localStorage")) return null;

  const cloudRegions = await get(cloudRegionsSelector);
  const region = window.localStorage.getItem(SELECTED_REGION_KEY);
  if (region && !cloudRegions.has(region)) {
    // If the selected region isn't valid, fall back to the first region
    const newPreferredRegion = cloudRegions.keys().next().value ?? null;
    return newPreferredRegion;
  }
  return region;
});

/** Takes our region ID (e.g. aws/us-east-1) and returns a url friendly slug aws-us-east-1 */
export const regionIdToSlug = (region: RegionId) => region.replace("/", "-");

/** Read only atom that returns a map of region slugs to IDs */
export const cloudRegionSlugToId = atom(async (get) => {
  const regions = await get(cloudRegionsSelector);
  return new Map(
    Array.from(regions.keys()).map((id) => [regionIdToSlug(id), id]),
  );
});

/** Gets the region ID for a slug, or undefined if the slug is not valid */
export const useRegionSlugToId = (slug: string) => {
  const [slugToIdMap] = useAtom(cloudRegionSlugToId);
  return slugToIdMap.get(slug);
};
