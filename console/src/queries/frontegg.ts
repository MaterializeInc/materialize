// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  DefaultError,
  useMutation,
  UseMutationOptions,
  useQuery,
  useQueryClient,
  useSuspenseQuery,
} from "@tanstack/react-query";
import React from "react";

import { hasTenantApiTokenPermissions } from "~/api/auth";
import {
  buildGlobalQueryKey,
  buildQueryKeyPart,
} from "~/api/buildQueryKeySchema";
import {
  createTenantApiToken,
  createUserApiToken,
  deleteTenantApiToken,
  deleteUserApiToken,
  fetchMfaPolicy,
  fetchTeamRoles,
  fetchTenantApiTokens,
  fetchUserApiTokens,
} from "~/api/frontegg";
import {
  ApiToken,
  NewApiToken,
  TenantApiToken,
  UserApiToken,
} from "~/api/frontegg/types";
import { User } from "~/external-library-wrappers/frontegg";

export const fronteggQueryKeys = {
  all: () => buildGlobalQueryKey("frontegg"),
  user: () => [...fronteggQueryKeys.all(), buildQueryKeyPart("user")] as const,
  apiTokens: () =>
    [...fronteggQueryKeys.all(), buildQueryKeyPart("api-tokens")] as const,
  listApiTokens: (params: { canAccessTenantApiTokens: boolean }) =>
    [...fronteggQueryKeys.apiTokens(), params] as const,
  createApiToken: () =>
    [...fronteggQueryKeys.apiTokens(), buildQueryKeyPart("create")] as const,
  deleteApiToken: () =>
    [...fronteggQueryKeys.apiTokens(), buildQueryKeyPart("delete")] as const,
  mfaPolicy: () =>
    [...fronteggQueryKeys.all(), buildQueryKeyPart("mfaPolicy")] as const,
  tenantRoles: () =>
    [...fronteggQueryKeys.all(), buildQueryKeyPart("tenantRoles")] as const,
};

export function useListApiTokens({ user }: { user: User }) {
  const canAccessTenantApiTokens = hasTenantApiTokenPermissions(user);
  return useSuspenseQuery({
    queryKey: fronteggQueryKeys.listApiTokens({ canAccessTenantApiTokens }),
    queryFn: async ({ signal }) => {
      const fetches: Promise<UserApiToken[] | TenantApiToken[]>[] = [
        fetchUserApiTokens({ signal }),
      ];
      if (canAccessTenantApiTokens) {
        fetches.push(fetchTenantApiTokens({ signal }));
      }
      const apiTokens = await Promise.all(fetches);
      return apiTokens
        .flat()
        .toSorted((x, y) => Date.parse(y.createdAt) - Date.parse(x.createdAt));
    },
  });
}

type CreateApiTokenVariables =
  | {
      type: "personal";
      description: string;
    }
  | {
      type: "service";
      description: string;
      user: string;
      roleIds: string[];
    };

function formatAppPassword({ clientId, secret }: NewApiToken) {
  const formattedClientId = clientId.replaceAll("-", "");
  const formattedSecret = secret.replaceAll("-", "");
  const password = `mzp_${formattedClientId}${formattedSecret}`;
  const obfuscatedPassword = `${new Array(password.length).fill("*").join("")}`;
  return { password, obfuscatedPassword };
}

export function useCreateApiToken(
  options?: UseMutationOptions<
    NewApiToken,
    DefaultError,
    CreateApiTokenVariables
  >,
) {
  const queryClient = useQueryClient();
  const mutation = useMutation({
    mutationKey: fronteggQueryKeys.createApiToken(),
    mutationFn: async (params: CreateApiTokenVariables) => {
      if (params.type === "personal") {
        return await createUserApiToken(params);
      } else {
        return await createTenantApiToken(params);
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: fronteggQueryKeys.apiTokens(),
      });
    },
    ...options,
  });

  const newPassword = React.useMemo(() => {
    if (!mutation.data) return null;

    const { password, obfuscatedPassword } = formatAppPassword(mutation.data);
    return { ...mutation.data, password, obfuscatedPassword };
  }, [mutation.data]);

  return {
    ...mutation,
    data: newPassword,
  };
}

export function useDeleteApiToken(
  options: UseMutationOptions<Response, DefaultError, { token: ApiToken }> = {},
) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationKey: fronteggQueryKeys.deleteApiToken(),
    mutationFn: async (params: { token: ApiToken }) => {
      if (params.token.type === "personal") {
        return await deleteUserApiToken({ id: params.token.clientId });
      } else {
        return await deleteTenantApiToken({ id: params.token.clientId });
      }
    },
    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: fronteggQueryKeys.apiTokens(),
      });
    },
    ...options,
  });
}

export function useMfaPolicy() {
  return useQuery({
    queryFn: ({ signal }) => fetchMfaPolicy({ signal }),
    queryKey: fronteggQueryKeys.mfaPolicy(),
    refetchInterval: 5000,
  });
}

export function useTeamRoles() {
  return useSuspenseQuery({
    queryKey: fronteggQueryKeys.tenantRoles(),
    queryFn: ({ signal }) => fetchTeamRoles({ signal }),
    refetchOnWindowFocus: false,
    staleTime: Infinity,
  });
}
