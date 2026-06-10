// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getBuildConstants } from "./buildConstants";
import { getImpersonatedEnvironment } from "./impersonation";

describe("getImpersonatedEnvironment", () => {
  it("returns the impersonated cloud provider, region, organizationId and environmentId", () => {
    expect(
      getImpersonatedEnvironment(
        "aws-us-west-1-t2frxsh2bbehbiq7ekwadtbyba-2.materialize.teleport.sh",
        getBuildConstants(),
      ),
    ).toEqual({
      provider: "aws",
      region: "us-west-1",
      regionId: "aws/us-west-1",
      organizationId: "9e8b1bc8-fa08-4870-a21f-22ac01cc3808",
      environmentId: "environment-9e8b1bc8-fa08-4870-a21f-22ac01cc3808-2",
      environmentdHttpAddress:
        "aws-us-west-1-t2frxsh2bbehbiq7ekwadtbyba-2.materialize.teleport.sh",
    });
  });

  it("throws a useful error", () => {
    expect(() =>
      getImpersonatedEnvironment(
        "aws-us-west-1-invalidBase32-2.materialize.teleport.sh",
        getBuildConstants(),
      ),
    ).toThrowError("Invalid impersonation subdomain");
  });
});
