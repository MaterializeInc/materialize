// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getCloudRegions } from "./cloudRegions";

describe("getCloudRegions", () => {
  it("returns impersonation info if set", () => {
    expect(
      getCloudRegions({
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
    ).toEqual([
      {
        provider: "aws",
        region: "us-west-1",
        regionApiUrl: "",
      },
    ]);
  });

  it("returns null when not impersonating, using kind or flexible deployment mode", () => {
    expect(
      getCloudRegions({
        impersonation: null,
      }),
    ).toEqual(null);
  });
});
