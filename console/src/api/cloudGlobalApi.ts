// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { formatRFC3339 } from "date-fns";
import createClient from "openapi-fetch";

import { NOT_SUPPORTED_MESSAGE } from "~/config/AppConfig";

import { apiClient } from "./apiClient";
import {
  handleOpenApiResponse,
  handleOpenApiResponseWithBody,
} from "./openApiUtils";
import { components, paths } from "./schemas/global-api";
import { OpenApiRequestOptions } from "./types";

export type Organization = components["schemas"]["OrganizationResponse"];
export type PlanType = components["schemas"]["PlanType"];
export type Invoice = components["schemas"]["Invoice"];
export type Region = components["schemas"]["Region"];
export type Regions = components["schemas"]["Regions"];
export type CreditBlock = components["schemas"]["CreditBlock"];
export type DailyCosts = components["schemas"]["DailyCostResponse"];
export type AllCosts = components["schemas"]["AllCosts"];
export type DailyCostKey = keyof components["schemas"]["AllCosts"];
export type Prices =
  | components["schemas"]["ComputePrice"]
  | components["schemas"]["StoragePrice"]
  | components["schemas"]["NetworkPrice"];
export type Marketplace = components["schemas"]["Marketplace"];
export type IntercomJwtResponse = components["schemas"]["IntercomJwtResponse"];
export type CreateLicenseKeyRequest =
  components["schemas"]["CreateLicenseKeyRequest"];
export type CreateLicenseKeyResponse =
  components["schemas"]["CreateLicenseKeyResponse"];
export type SelfManagedSubscriptionResponse =
  components["schemas"]["GetSelfManagedSubscriptionResponse"];

const client =
  apiClient.type === "cloud"
    ? createClient<paths>({
        baseUrl: apiClient.cloudGlobalApiBasePath,
        fetch: apiClient.cloudApiFetch,
      })
    : null;

const getClient = () => {
  if (client === null) {
    throw new Error(NOT_SUPPORTED_MESSAGE);
  }
  return client;
};

export async function getCurrentOrganization(
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET("/api/organization", {
    signal: requestOptions?.signal,
    headers,
    ...options,
  });
  return handleOpenApiResponseWithBody(data, response);
}

export async function onboardCurrentOrganization(
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().POST(
    "/api/organization/onboarded",
    {
      signal: requestOptions?.signal,
      headers,
      ...options,
    },
  );
  return handleOpenApiResponse(data, response);
}

export async function recentInvoices(
  params: {
    draft?: boolean;
    issued?: boolean;
    paid?: boolean;
    void?: boolean;
    synced?: boolean;
  } = {},
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET("/api/invoices", {
    params: { query: params },
    signal: requestOptions?.signal,
    headers,
    ...options,
  });
  return handleOpenApiResponseWithBody(data, response);
}
// The /cloud-regions API returns localhost URLs without a URL scheme,
// unlike staging and production which include proper schemes.
// This should be fixed on the cloud side, but for now we prepend "http://"
// for our local kind environments to ensure calls to the API work.
function mapCloudRegions(regions: components["schemas"]["Region"][]) {
  return regions.map((regionInfo) => {
    if (regionInfo.url.startsWith("localhost")) {
      return {
        ...regionInfo,
        url: regionInfo.url.replace("localhost", "http://localhost"),
      };
    }
    return regionInfo;
  });
}

export async function getCloudRegions(
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET("/api/cloud-regions", {
    signal: requestOptions?.signal,
    headers,
    ...options,
  });

  const res = await handleOpenApiResponseWithBody(data, response);

  return {
    ...res,
    data: { ...res.data, data: mapCloudRegions(res.data.data) },
  };
}

export async function getCredits(requestOptions: OpenApiRequestOptions = {}) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET("/api/credits", {
    signal: requestOptions?.signal,
    headers,
    ...options,
  });
  return handleOpenApiResponseWithBody(data, response);
}

export async function getDailyCosts(
  startDate: Date,
  endDate: Date,
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET("/api/costs/daily", {
    params: {
      query: {
        startDate: formatRFC3339(startDate),
        endDate: formatRFC3339(endDate),
      },
    },
    signal: requestOptions?.signal,
    headers,
    ...options,
  });
  return handleOpenApiResponseWithBody(data, response);
}

export async function createStripeSetupIntent(
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().POST(
    "/api/organization/payment-methods/setup-intent",
    {
      signal: requestOptions?.signal,
      headers,
      ...options,
    },
  );
  return handleOpenApiResponseWithBody(data, response);
}

export async function setDefaultPaymentMethod(
  paymentMethodId: string,
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().POST(
    "/api/organization/payment-methods/default",
    {
      signal: requestOptions?.signal,
      headers,
      body: { paymentMethodId },
      ...options,
    },
  );
  return handleOpenApiResponse(data, response);
}

export async function detachPaymentMethod(
  paymentMethodId: string,
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().POST(
    "/api/organization/payment-methods/detach",
    {
      signal: requestOptions?.signal,
      headers,
      body: { paymentMethodId },
      ...options,
    },
  );
  return handleOpenApiResponse(data, response);
}

export async function issueIntercomJwt(
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().POST(
    "/api/organization/intercom/jwt",
    {
      signal: requestOptions?.signal,
      headers,
      ...options,
    },
  );
  return handleOpenApiResponseWithBody(data, response);
}

export async function issueLicenseKey(
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().POST("/api/license-key", {
    signal: requestOptions?.signal,
    headers,
    body: {
      useCaseId: null, // Optional field, can be null
    },
    ...options,
  });
  return handleOpenApiResponseWithBody(data, response);
}

export async function issueCommunityLicenseKey(
  email: string,
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().POST(
    "/api/community-license-key",
    {
      signal: requestOptions?.signal,
      headers,
      body: {
        email,
      },
      ...options,
    },
  );
  return handleOpenApiResponseWithBody(data, response);
}

export async function getSelfManagedSubscription(
  useCaseId: string | null,
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET(
    "/api/self-managed-subscription",
    {
      params: {
        path: {
          useCaseId,
        },
      },
      signal: requestOptions?.signal,
      headers,
      ...options,
    },
  );
  return handleOpenApiResponseWithBody(data, response);
}
