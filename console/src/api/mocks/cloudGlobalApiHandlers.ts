// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { http, HttpResponse } from "msw";

import {
  CreditBlock,
  DailyCostBreakdown,
  Invoice,
  Organization,
  Region,
  Regions,
} from "~/api/cloudGlobalApi";

export const buildCloudRegionsReponse = (
  options: {
    regions?: Region[];
    status?: number;
  } = {},
) =>
  http.get("*/api/cloud-regions", () => {
    const response: Regions = {
      data: options.regions ?? [
        {
          id: "aws/eu-west-1",
          name: "eu-west-1",
          cloudProvider: "aws",
          url: "https://api.eu-west-1.aws.test.cloud.materialize.com",
        },
        {
          id: "aws/us-east-1",
          name: "us-east-1",
          cloudProvider: "aws",
          url: "https://api.us-east-1.aws.test.cloud.materialize.com",
        },
      ],
    };
    return HttpResponse.json(response, { status: options.status ?? 200 });
  });

export const buildDailyCostBreakdownResponse = (
  options: { payload?: DailyCostBreakdown; status?: number } = {},
) =>
  http.get("*/api/costs/breakdown/daily", () => {
    const payload: DailyCostBreakdown = options.payload ?? { days: [] };
    return HttpResponse.json(payload, { status: options.status ?? 200 });
  });

export const buildInvoicesResponse = (
  options: { invoices?: Invoice[]; status?: number } = {},
) =>
  http.get("*/api/invoices", () => {
    const response = {
      data: options.invoices ?? [],
      nextCursor: null,
    };
    return HttpResponse.json(response, { status: options.status ?? 200 });
  });

export const buildCreditsResponse = (
  options: { payload?: CreditBlock[]; status?: number } = {},
) =>
  http.get("*/api/credits", () => {
    let payload = options.payload;
    if (!payload) {
      payload = [
        {
          balance: 10,
          perUnitCostBasis: "1.00",
        },
      ];
    }
    return HttpResponse.json(payload, { status: options.status ?? 200 });
  });

export const buildCloudOrganizationsResponse = (
  options: { payload?: Organization; status?: number } = {},
) =>
  http.get("*/api/organization", () => {
    let payload = options.payload;
    if (!payload) {
      payload = {
        id: "org_id",
        subscription: { type: "capacity", marketplace: "direct" },
        name: "Test Organization",
        blocked: false,
        onboarded: true,
        trialExpiresAt: null,
      };
    }
    return HttpResponse.json(payload, { status: options.status ?? 200 });
  });

export default [buildCloudRegionsReponse(), buildCloudOrganizationsResponse()];
