// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { getCloudGlobalApiUrl, getFronteggUrl } from "./apiUrls";

describe("getCloudGlobalApiUrl", () => {
  it("production stack should return the production url", () => {
    expect(
      getCloudGlobalApiUrl({ stack: "production", isImpersonation: false }),
    ).toEqual("https://api.cloud.materialize.com");
    expect(
      getCloudGlobalApiUrl({ stack: "production", isImpersonation: true }),
    ).toEqual(
      "https://global-api-aws-us-east-1-production.materialize.teleport.sh",
    );
  });

  it("staging stack should return the staging url", () => {
    expect(
      getCloudGlobalApiUrl({ stack: "staging", isImpersonation: false }),
    ).toEqual("https://api.staging.cloud.materialize.com");
    expect(
      getCloudGlobalApiUrl({ stack: "staging", isImpersonation: true }),
    ).toEqual(
      "https://global-api-aws-us-east-1-staging.materialize.teleport.sh",
    );
  });

  it("local stack should return the local url", () => {
    expect(
      getCloudGlobalApiUrl({ stack: "local", isImpersonation: false }),
    ).toEqual("http://127.0.0.1:32002");
    expect(
      getCloudGlobalApiUrl({ stack: "local", isImpersonation: true }),
    ).toEqual("http://127.0.0.1:32002");
  });

  it("personal stack should return the personal stack url", () => {
    expect(
      getCloudGlobalApiUrl({ stack: "someuser.dev", isImpersonation: false }),
    ).toEqual("https://api.someuser.dev.cloud.materialize.com");
    expect(
      getCloudGlobalApiUrl({ stack: "someuser.dev", isImpersonation: true }),
    ).toEqual(
      "https://global-api-aws-us-east-1-someuser-dev.materialize.teleport.sh",
    );
  });
});

describe("getFronteggUrl", () => {
  it("production stack should return the production url", () => {
    expect(getFronteggUrl("production")).toEqual(
      "https://admin.cloud.materialize.com",
    );
  });

  it("staging stack should return the staging url", () => {
    expect(getFronteggUrl("staging")).toEqual(
      "https://admin.staging.cloud.materialize.com",
    );
  });

  it("local stack should return the staging url", () => {
    expect(getFronteggUrl("local")).toEqual(
      "https://admin.staging.cloud.materialize.com",
    );
  });

  it("personal stack should return the personal stack url", () => {
    expect(getFronteggUrl("someuser.dev")).toEqual(
      "https://admin.someuser.dev.cloud.materialize.com",
    );
  });
});

describe("getFronteggUrl", () => {
  it("production stack should return the production url", () => {
    expect(getFronteggUrl("production")).toEqual(
      "https://admin.cloud.materialize.com",
    );
  });

  it("staging stack should return the staging url", () => {
    expect(getFronteggUrl("staging")).toEqual(
      "https://admin.staging.cloud.materialize.com",
    );
  });

  it("local stack should return the staging url", () => {
    expect(getFronteggUrl("local")).toEqual(
      "https://admin.staging.cloud.materialize.com",
    );
  });

  it("personal stack should return the personal stack url", () => {
    expect(getFronteggUrl("someuser.dev")).toEqual(
      "https://admin.someuser.dev.cloud.materialize.com",
    );
  });
});
