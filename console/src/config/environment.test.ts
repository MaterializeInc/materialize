// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getEnvironmentdConfig } from "./environment";

describe("getEnvironmentdConfig", () => {
  it("returns impersonation info if set", () => {
    expect(
      getEnvironmentdConfig({
        consoleHost: "localhost:3000",
        cloudRegionOverride: {
          provider: "aws",
          region: "us-est-1",
          regionApiUrl: "",
        },
        impersonation: {
          provider: "aws",
          region: "us-west-1",
          regionId: "aws/us-west-1",
          organizationId: "9e8b1bc8-fa08-4870-a21f-22ac01cc3808",
          environmentId: "environment-9e8b1bc8-fa08-4870-a21f-22ac01cc3808-2",
          environmentdHttpAddress:
            "aws-us-west-1-t2frxsh2bbehbiq7ekwadtbyba-2.materialize.teleport.sh",
        },
      }),
    ).toEqual({
      environmentdHttpAddress:
        "aws-us-west-1-t2frxsh2bbehbiq7ekwadtbyba-2.materialize.teleport.sh",
      regionId: "aws/us-west-1",
    });
  });

  it("returns console host and cloud region overrides outside impersonation", () => {
    expect(
      getEnvironmentdConfig({
        consoleHost: "localhost:3000",
        cloudRegionOverride: {
          provider: "",
          region: "local",
          regionApiUrl: "",
        },
        impersonation: null,
      }),
    ).toEqual({
      environmentdHttpAddress: "localhost:3000",
      regionId: "/local",
    });
  });

  it("returns null when envd host, impersonation and cloud regions are not set", () => {
    expect(
      getEnvironmentdConfig({
        consoleHost: "console.materialize.com",
        cloudRegionOverride: undefined,
        impersonation: null,
      }),
    ).toEqual(null);
  });
});
