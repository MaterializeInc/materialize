// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { QueryKey, useQuery } from "@tanstack/react-query";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import { fetchLicenseKeys } from "~/api/materialize/license/licenseKeys";

export const licenseKeysQueryKeys = {
  all: () => buildRegionQueryKey("licenseKeys"),
  detail: () =>
    [...licenseKeysQueryKeys.all(), buildQueryKeyPart("detail")] as const,
};

export function useLicenseKey() {
  const { data, isLoading, isError } = useQuery({
    queryKey: licenseKeysQueryKeys.detail(),
    queryFn: ({
      queryKey,
      signal,
    }: {
      queryKey: QueryKey;
      signal?: AbortSignal;
    }) => {
      return fetchLicenseKeys({
        queryKey,
        requestOptions: { signal },
      });
    },
  });

  return {
    data,
    isLoading,
    isError,
  };
}
