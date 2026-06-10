// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { addDays, differenceInDays, formatISO, parseISO } from "date-fns";
import { http, HttpResponse } from "msw";

import {
  CreditBlock,
  DailyCosts,
  Invoice,
  Organization,
  Region,
  Regions,
} from "~/api/cloudGlobalApi";
import { assert } from "~/util";

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

export function generateDailyCostResponsePayload(
  startDate: Date,
  endDate: Date,
) {
  const windowDuration = differenceInDays(endDate, startDate);
  const days: DailyCosts["daily"] = [];
  for (let dayOffset = 0; dayOffset < windowDuration; dayOffset++) {
    const storagePrices: DailyCosts["daily"][0]["costs"]["storage"]["prices"] =
      [];
    const computePrices: DailyCosts["daily"][0]["costs"]["compute"]["prices"] =
      [];
    let computeSubtotal = 0,
      storageSubtotal = 0;
    for (const regionId of ["aws/us-east-1", "aws/eu-west-1"]) {
      for (const replicaSize of ["xsmall", "small", "medium"]) {
        const subtotal = Math.random() * 10_000;
        computeSubtotal += subtotal;
        computePrices.push({
          unitAmount: "1.00",
          regionId,
          replicaSize,
          subtotal: subtotal.toString(),
        });
      }
      const subtotal = Math.random() * 10;
      storageSubtotal += subtotal;
      storagePrices.push({
        subtotal: subtotal.toString(),
        regionId: regionId,
        unitAmount: "1.00",
      });
    }
    days.push({
      costs: {
        storage: {
          subtotal: storageSubtotal.toString(),
          total: storageSubtotal.toString(),
          prices: storagePrices,
        },
        compute: {
          subtotal: computeSubtotal.toString(),
          total: computeSubtotal.toString(),
          prices: computePrices,
        },
      },
      subtotal: (computeSubtotal + storageSubtotal).toString(),
      total: (computeSubtotal + storageSubtotal).toString(),
      startDate: formatISO(addDays(startDate, dayOffset)),
      endDate: formatISO(addDays(startDate, dayOffset + 1)),
    });
  }
  const subtotal = days.reduce(
    (prevValue, day) => parseFloat(day.subtotal) + prevValue,
    0,
  );
  const response: DailyCosts = {
    subtotal: subtotal.toString(),
    total: subtotal.toString(),
    daily: days,
  };
  return response;
}

export const buildDailyCostResponse = (
  options: { payload?: DailyCosts; status?: number } = {},
) =>
  http.get("*/api/costs/daily", (info) => {
    let payload = options.payload;
    if (!payload) {
      const url = new URL(info.request.url);
      const startParam = url.searchParams.get("startDate");
      assert(startParam !== null);
      const startDate = parseISO(startParam);
      const endParam = url.searchParams.get("endDate");
      assert(endParam !== null);
      const endDate = parseISO(endParam);
      payload = generateDailyCostResponsePayload(startDate, endDate);
    }
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
