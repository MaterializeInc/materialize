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
import { addUtcDays } from "~/util";
import { formatDateInUtc } from "~/utils/dateFormat";

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
export type AvailableRegion = components["schemas"]["AvailableRegion"];
export type AvailableRegions =
  components["schemas"]["Paginated_AvailableRegion"];
export type Region =
  | components["schemas"]["Region"]
  | { regionInfo?: null; regionState: "disabled" };
export type RegionInfo = NonNullable<Region["regionInfo"]>;
export type RegionRequest = components["schemas"]["RegionRequest"];
export type CreditBlock = components["schemas"]["CreditBlock"];
export type CostBreakdownAccount =
  components["schemas"]["CostBreakdownAccount"];
export type CostBreakdownCluster =
  components["schemas"]["CostBreakdownCluster"];
export type CostBreakdownDay = components["schemas"]["CostBreakdownDay"];
export type DailyCostBreakdown =
  components["schemas"]["DailyCostBreakdownResponse"];
export type Marketplace = components["schemas"]["Marketplace"];
export type IntercomJwtResponse = components["schemas"]["IntercomJwtResponse"];
export type SupportTicket = components["schemas"]["SupportTicket"];
export type SupportRegion = components["schemas"]["SupportRegion"];
export type SupportTicketFile = components["schemas"]["SupportTicketFile"];
/** Global APIs older than cloud#13912's regions don't return `regions`. */
export type SupportOrganization = Omit<
  components["schemas"]["SupportOrganization"],
  "regions"
> & { regions?: SupportRegion[] };
export type CreatedSupportTicket =
  components["schemas"]["CreatedSupportTicket"];
export type SupportTicketDetail = components["schemas"]["SupportTicketDetail"];
export type SupportTicketMessage =
  components["schemas"]["SupportTicketMessage"];
export type SupportTicketSeverity =
  components["schemas"]["SupportTicketSeverity"];
export type SupportTicketEnvironmentType =
  components["schemas"]["SupportTicketEnvironmentType"];
export type CreateSupportTicketRequest =
  components["schemas"]["CreateSupportTicketRequest"];
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
export async function getCloudRegions(
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET("/api/cloud-regions", {
    signal: requestOptions?.signal,
    headers,
    ...options,
  });
  return handleOpenApiResponseWithBody(data, response);
}

// NOTE: The spec declares this route as `/api/region/:cloud_provider/:region`
// without path parameters, and openapi-fetch only substitutes `{name}`
// placeholders, so the region calls build their URL here.
async function fetchRegion(
  cloudProvider: string,
  region: string,
  init: RequestInit,
) {
  if (apiClient.type !== "cloud") {
    throw new Error(NOT_SUPPORTED_MESSAGE);
  }
  const path = `/api/region/${encodeURIComponent(cloudProvider)}/${encodeURIComponent(region)}`;
  return apiClient.cloudApiFetch(
    `${apiClient.cloudGlobalApiBasePath}${path}`,
    init,
  );
}

async function readRegionBody(response: Response): Promise<Region | undefined> {
  if (!response.ok) {
    return undefined;
  }
  const text = await response.text();
  return text ? (JSON.parse(text) as Region) : undefined;
}

export async function createRegion(
  cloudProvider: string,
  region: string,
  body: RegionRequest = {},
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const response = await fetchRegion(cloudProvider, region, {
    ...options,
    method: "PATCH",
    headers: { "Content-Type": "application/json", ...headers },
    body: JSON.stringify(body),
  });
  return handleOpenApiResponse(await readRegionBody(response), response);
}

export async function getRegion(
  cloudProvider: string,
  region: string,
  requestOptions: OpenApiRequestOptions = {},
): Promise<ReturnType<typeof handleOpenApiResponseWithBody<Region>>> {
  const response = await fetchRegion(cloudProvider, region, requestOptions);
  if (response.status === 204) {
    return { data: { regionState: "disabled" as const }, ...response };
  }
  return handleOpenApiResponseWithBody(
    await readRegionBody(response),
    response,
  );
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

export async function getCostsBreakdownDaily(
  startDate: Date,
  endDate: Date,
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET(
    "/api/costs/breakdown/daily",
    {
      params: {
        query: {
          // Both query params are inclusive calendar days (bare `YYYY-MM-DD`,
          // no time component — see `DateRange` in cloud's interface.rs),
          // unlike every other date param on this client. `startDate`/`endDate`
          // here are the internal [start, end) convention used throughout the
          // console, so the exclusive `endDate` becomes the day before it.
          startDate: formatDateInUtc(startDate, "yyyy-MM-dd"),
          endDate: formatDateInUtc(addUtcDays(endDate, -1), "yyyy-MM-dd"),
        },
      },
      signal: requestOptions?.signal,
      headers,
      ...options,
    },
  );
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

export async function listSupportTickets(
  { cursor, limit }: { cursor?: string; limit?: number } = {},
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET(
    "/api/organization/support/tickets",
    {
      params: {
        query: {
          ...(cursor ? { cursor } : {}),
          ...(limit ? { limit } : {}),
        },
      },
      signal: requestOptions?.signal,
      headers,
      ...options,
    },
  );
  return handleOpenApiResponseWithBody(data, response);
}

export async function listSupportOrganizations(
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET(
    "/api/organization/support/organizations",
    {
      signal: requestOptions?.signal,
      headers,
      ...options,
    },
  );
  return handleOpenApiResponseWithBody(data, response);
}

export async function getSupportTicket(
  ticketId: string,
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET(
    "/api/organization/support/tickets/{ticket_id}",
    {
      params: { path: { ticket_id: ticketId } },
      signal: requestOptions?.signal,
      headers,
      ...options,
    },
  );
  return handleOpenApiResponseWithBody(data, response);
}

export async function createSupportTicket(
  ticket: CreateSupportTicketRequest,
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().POST(
    "/api/organization/support/tickets",
    {
      signal: requestOptions?.signal,
      headers,
      body: ticket,
      ...options,
    },
  );
  return handleOpenApiResponseWithBody(data, response);
}

/** Adds files to one of the user's tickets, as a reply from them. */
export async function attachSupportTicketFiles(
  ticketId: string,
  files: SupportTicketFile[],
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().POST(
    "/api/organization/support/tickets/{ticket_id}/attachments",
    {
      signal: requestOptions?.signal,
      headers,
      params: { path: { ticket_id: ticketId } },
      body: { files },
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
  environmentId: string | null,
  requestOptions: OpenApiRequestOptions = {},
) {
  const { headers, ...options } = requestOptions;
  const { data, response } = await getClient().GET(
    "/api/self-managed-subscription",
    {
      params: {
        path: {
          environmentId,
        },
      },
      signal: requestOptions?.signal,
      headers,
      ...options,
    },
  );
  return handleOpenApiResponseWithBody(data, response);
}
