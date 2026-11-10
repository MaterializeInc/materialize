// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { http, HttpResponse } from "msw";

import { apiClient } from "./apiClient";
import { createRegion, getRegion } from "./cloudGlobalApi";
import server from "./mocks/server";

const usEast1Url = () => {
  if (apiClient.type !== "cloud") {
    throw new Error("expected the cloud API client");
  }
  return `${apiClient.cloudGlobalApiBasePath}/api/region/aws/us-east-1`;
};

describe("region endpoints", () => {
  it("gets the region from the global API's provider and region path", async () => {
    let requestedUrl: string | undefined;
    server.use(
      http.get("*/api/region/:cloudProvider/:region", ({ request }) => {
        requestedUrl = request.url;
        return new HttpResponse(null, { status: 204 });
      }),
    );

    const { data } = await getRegion("aws", "us-east-1");

    expect(requestedUrl).toBe(usEast1Url());
    expect(data).toEqual({ regionState: "disabled" });
  });

  it("creates the region with a JSON PATCH to the same path", async () => {
    let received:
      | {
          url: string;
          method: string;
          contentType: string | null;
          body: unknown;
        }
      | undefined;
    server.use(
      http.patch("*/api/region/:cloudProvider/:region", async ({ request }) => {
        received = {
          url: request.url,
          method: request.method,
          contentType: request.headers.get("content-type"),
          body: await request.json(),
        };
        return HttpResponse.json(
          { regionState: "enablement-pending" },
          { status: 202 },
        );
      }),
    );

    await createRegion("aws", "us-east-1");

    expect(received).toEqual({
      url: usEast1Url(),
      method: "PATCH",
      contentType: "application/json",
      body: {},
    });
  });
});
