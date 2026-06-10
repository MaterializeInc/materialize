// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { http, HttpResponse } from "msw";

import { Region, RegionInfo } from "~/api/cloudRegionApi";

export default [
  http.get("*/api/region", () => {
    return new HttpResponse(null, { status: 204 });
  }),
];

export const buildPendingRegionResponse = (
  regionState: Exclude<Region["regionState"], "disabled">,
  regionInfo?: RegionInfo,
  status = 202,
) =>
  http.get("*/api/region", () => {
    const response: Region = {
      regionInfo,
      regionState,
    };
    return HttpResponse.json(response, { status });
  });
export const buildEnabledRegionReponse = (
  regionInfo: Partial<RegionInfo> = {},
  status = 200,
) =>
  http.get("*/api/region", () => {
    const response: Region = {
      regionInfo: {
        sqlAddress:
          "47azz5yc00ab7xnu21b4p2evh.eu-west-1.aws.staging.materialize.cloud:6875",
        httpAddress:
          "47azz5yc00ab7xnu21b4p2evh.eu-west-1.aws.staging.materialize.cloud:443",
        resolvable: true,
        enabledAt: "2022-11-11T00:20:14Z",
        ...regionInfo,
      },
      regionState: "enabled",
    };
    return HttpResponse.json(response, { status });
  });

export const buildServerErrorRegionResponse = (
  error = "Server error",
  status = 500,
) =>
  http.get("*/api/region", () => {
    return HttpResponse.text(error, { status });
  });

export const buildNetworkErrorRegionResponse = () =>
  http.get("*/api/region", () => {
    return HttpResponse.error();
  });
