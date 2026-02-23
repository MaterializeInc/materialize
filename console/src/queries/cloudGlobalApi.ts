// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useMutation } from "@tanstack/react-query";

import {
  buildGlobalQueryKey,
  buildQueryKeyPart,
} from "~/api/buildQueryKeySchema";
import {
  getCloudRegions,
  issueCommunityLicenseKey,
  issueLicenseKey,
} from "~/api/cloudGlobalApi";
import { components } from "~/api/schemas/global-api";
import { getQueryClient } from "~/queryClient";

export type Region = components["schemas"]["Region"];

export const cloudGlobalApiQueryKeys = {
  all: () => buildGlobalQueryKey("cloudGlobalApi"),
  cloudRegions: () =>
    [
      ...cloudGlobalApiQueryKeys.all(),
      buildQueryKeyPart("cloudRegions"),
    ] as const,
  licenseKey: () => ["cloudGlobalApi", "licenseKey"] as const,
  communityLicenseKey: () => ["cloudGlobalApi", "communityLicenseKey"] as const,
};

export function fetchCloudRegions() {
  return getQueryClient().fetchQuery({
    queryKey: cloudGlobalApiQueryKeys.cloudRegions(),
    staleTime: Infinity,
    gcTime: Infinity,
    queryFn: async ({ signal }) => {
      const response = await getCloudRegions({ signal });
      return response.data;
    },
  });
}

export function useIssueLicenseKey() {
  return useMutation({
    mutationKey: cloudGlobalApiQueryKeys.licenseKey(),
    mutationFn: async () => {
      const response = await issueLicenseKey();
      return response.data;
    },
  });
}

export function useIssueCommunityLicenseKey() {
  return useMutation({
    mutationKey: cloudGlobalApiQueryKeys.communityLicenseKey(),
    mutationFn: async (email: string) => {
      const response = await issueCommunityLicenseKey(email);
      return response.data;
    },
  });
}
