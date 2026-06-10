// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { NOT_SUPPORTED_MESSAGE } from "~/config/AppConfig";
import { ITeamUserRole } from "~/external-library-wrappers/frontegg";

import { apiClient } from "../apiClient";
import {
  MfaPolicy,
  MfaPolicyResponse,
  NewTenantApiToken,
  NewUserApiToken,
  TenantApiToken,
  UserApiToken,
} from "./types";

function handleFronteggResponse(response: Response): Response {
  if (!response.ok) {
    throw new Error(`Frontegg failed to fetch. ${response.text()}`);
  }
  return response;
}

/** Prepends a path the frontegg origin */
export function buildFronteggUrl(path: string) {
  if (apiClient.type === "cloud" && !apiClient.isImpersonating) {
    return apiClient.fronteggApiBasePath + path;
  }
  throw new Error(NOT_SUPPORTED_MESSAGE);
}

const fetchWithAuth = (...args: Parameters<typeof fetch>) => {
  if (apiClient.type !== "cloud") {
    throw new Error(NOT_SUPPORTED_MESSAGE);
  }
  return apiClient.cloudApiFetch(...args);
};

export async function fetchUserApiTokens(requestOptions?: RequestInit) {
  const response = handleFronteggResponse(
    await fetchWithAuth(
      buildFronteggUrl("/frontegg/identity/resources/users/api-tokens/v1"),
      requestOptions,
    ),
  );

  const responseJson = await response.json();

  return responseJson.map((props: any) => ({
    ...props,
    type: "personal",
  })) as Promise<Array<UserApiToken>>;
}

export async function createUserApiToken(
  params: { description: string },
  requestOptions?: RequestInit,
) {
  const response = handleFronteggResponse(
    await fetchWithAuth(
      buildFronteggUrl("/frontegg/identity/resources/users/api-tokens/v1"),
      {
        method: "post",
        body: JSON.stringify(params),
        ...requestOptions,
      },
    ),
  );

  return response.json() as Promise<NewUserApiToken>;
}

export async function deleteUserApiToken(
  params: { id: string },
  requestOptions?: RequestInit,
) {
  return handleFronteggResponse(
    await fetchWithAuth(
      buildFronteggUrl(
        `/frontegg/identity/resources/users/api-tokens/v1/${params.id}`,
      ),
      {
        method: "delete",
        ...requestOptions,
      },
    ),
  );
}

export async function fetchTenantApiTokens(requestOptions?: RequestInit) {
  const response = handleFronteggResponse(
    await fetchWithAuth(
      buildFronteggUrl("/frontegg/identity/resources/tenants/api-tokens/v1"),
      requestOptions,
    ),
  );
  const responseJson = await response.json();
  return responseJson.map((props: any) => ({
    ...props,
    type: "service",
    user: props.metadata?.user,
  })) as Promise<Array<TenantApiToken>>;
}

export async function createTenantApiToken(
  { user, ...params }: { description: string; user: string; roleIds: string[] },
  requestOptions?: RequestInit,
) {
  const response = handleFronteggResponse(
    await fetchWithAuth(
      buildFronteggUrl("/frontegg/identity/resources/tenants/api-tokens/v1"),
      {
        method: "post",
        body: JSON.stringify({
          metadata: {
            user,
          },
          ...params,
        }),
        ...requestOptions,
      },
    ),
  );

  return response.json() as Promise<NewTenantApiToken>;
}

export async function deleteTenantApiToken(
  params: { id: string },
  requestOptions?: RequestInit,
) {
  return handleFronteggResponse(
    await fetchWithAuth(
      buildFronteggUrl(
        `/frontegg/identity/resources/tenants/api-tokens/v1/${params.id}`,
      ),
      {
        method: "delete",
        ...requestOptions,
      },
    ),
  );
}

export async function fetchMfaPolicy(
  requestOptions?: RequestInit,
): Promise<MfaPolicyResponse> {
  const response = await fetchWithAuth(
    buildFronteggUrl(
      "/frontegg/identity/resources/configurations/v1/mfa-policy",
    ),
    requestOptions,
  );
  const responseJson = await response.json();
  if (!response.ok) {
    return { enforceMFAType: "NotSet" };
  }
  return responseJson as Promise<MfaPolicy>;
}

// Function to fetch the roles of the current tenant/team. We use this
// to get all available roles of the tenant when creating a tenant API token.
export async function fetchTeamRoles(requestOptions?: RequestInit) {
  const response = await fetchWithAuth(
    buildFronteggUrl(`/frontegg/team/resources/roles/v1`),
    requestOptions,
  );

  if (response.status === 403) {
    // If the user does not have access to the team roles, we return an empty array and assume
    // the UI disables access to creating a tenant API token.
    return [];
  }

  const responseJson = (await response.json()) as {
    items: ITeamUserRole[];
  };

  return responseJson.items;
}
