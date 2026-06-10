// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import createClient from "openapi-fetch";

import { NOT_SUPPORTED_MESSAGE } from "~/config/AppConfig";

import { apiClient } from "./apiClient";
import {
  handleOpenApiResponse,
  handleOpenApiResponseWithBody,
} from "./openApiUtils";
import { components, paths } from "./schemas/region-api";
import { OpenApiRequestOptions } from "./types";

export type Region =
  | components["schemas"]["Region"]
  | { regionInfo?: null; regionState: "disabled" };
export type RegionInfo = NonNullable<Region["regionInfo"]>;
export type RegionRequest = components["schemas"]["RegionRequest"];

export async function createRegion(
  baseUrl: string,
  body: RegionRequest = {},
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const client =
    apiClient.type === "cloud"
      ? createClient<paths>({
          baseUrl,
          fetch: apiClient.cloudApiFetch,
        })
      : null;

  if (client === null) {
    throw new Error(NOT_SUPPORTED_MESSAGE);
  }

  const { data, response } = await client.PATCH("/api/region", {
    headers: {
      ...headers,
    },
    ...options,
    body,
  });
  return handleOpenApiResponse(data, response);
}

export async function getRegion(
  baseUrl: string,
  requestOptions: OpenApiRequestOptions = {},
): Promise<ReturnType<typeof handleOpenApiResponseWithBody<Region>>> {
  const { headers, ...options } = requestOptions;
  const client =
    apiClient.type === "cloud"
      ? createClient<paths>({
          baseUrl,
          fetch: apiClient.cloudApiFetch,
        })
      : null;

  if (client === null) {
    throw new Error(NOT_SUPPORTED_MESSAGE);
  }
  const { data, response } = await client.GET("/api/region", {
    headers,
    ...options,
  });
  if (response.status === 204) {
    return { data: { regionState: "disabled" as const }, ...response };
  }
  return handleOpenApiResponseWithBody(data, response);
}
