// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import {
  buildQueryKeyPart,
  buildRegionQueryKey,
} from "~/api/buildQueryKeySchema";
import {
  createSecret,
  CreateSecretVariables,
} from "~/api/materialize/secret/createSecrets";
import {
  fetchSecretsList,
  ListFilters,
} from "~/api/materialize/secret/fetchSecretsList";

export const secretQueryKeys = {
  all: () => buildRegionQueryKey("secrets"),
  list: ({ databaseId, schemaId, nameFilter }: ListFilters = {}) =>
    [
      ...secretQueryKeys.all(),
      buildQueryKeyPart("list", {
        databaseId: databaseId ?? "",
        schemaId: schemaId ?? "",
        nameFilter: nameFilter ?? "",
      }),
    ] as const,
  create: () =>
    [...secretQueryKeys.all(), buildQueryKeyPart("create")] as const,
};

export function useSecretsListPage(filters: ListFilters) {
  return useQuery({
    queryKey: secretQueryKeys.list(filters),
    queryFn: ({ queryKey, signal }) => {
      const [, filtersKeyPart] = queryKey;
      return fetchSecretsList({
        queryKey,
        filters: filtersKeyPart,
        requestOptions: { signal },
      });
    },
  });
}

export function useCreateSecret() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationKey: secretQueryKeys.create(),
    mutationFn: (variables: CreateSecretVariables) => {
      return createSecret({ variables, queryKey: secretQueryKeys.create() });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: secretQueryKeys.all(),
      });
    },
  });
}
