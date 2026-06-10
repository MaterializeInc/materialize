// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { describe, expect, it } from "vitest";

import { getQueryBuilderForVersion } from "./versioning";

describe("getQueryBuilderForVersion", () => {
  // Mock query builder factories that return a simple object for identification.
  // This mimics the behavior of the real query builders
  // which accept arguments and return a query object.
  const v1Query = ((args?: any) => ({ queryId: "v1", args })) as any;
  const v2Query = ((args?: any) => ({ queryId: "v2", args })) as any;
  const v3Query = ((args?: any) => ({ queryId: "v3", args })) as any;
  const fallbackQuery = ((args?: any) => ({
    queryId: "fallback",
    args,
  })) as any;

  const versionMap = {
    "1.0.0": v1Query,
    "2.0.0": v2Query,
    "3.0.0": v3Query,
    "0.0.0": fallbackQuery,
  };

  it("should select the exact version when available", () => {
    const result = getQueryBuilderForVersion("2.0.0", versionMap);
    expect(result).toBe(v2Query);
  });

  it("should select the highest version that is less than or equal to the environment version", () => {
    const result = getQueryBuilderForVersion("2.5.0", versionMap);
    expect(result).toBe(v2Query);
  });

  it("should fall back to '0.0.0' for versions older than any in the map", () => {
    const result = getQueryBuilderForVersion("0.5.0", versionMap);
    expect(result).toBe(fallbackQuery);
  });

  it("should default to the latest version if environmentVersion is undefined", () => {
    const result = getQueryBuilderForVersion(undefined, versionMap);
    expect(result).toBe(v3Query);
  });

  it("should default to the latest version if environmentVersion is an invalid semver", () => {
    const result = getQueryBuilderForVersion("invalid-version", versionMap);
    expect(result).toBe(v3Query);
  });

  it("should handle maps with only a '0.0.0' version", () => {
    const fallbackOnlyMap = { "0.0.0": fallbackQuery };
    const result = getQueryBuilderForVersion("1.0.0", fallbackOnlyMap);
    expect(result).toBe(fallbackQuery);
  });
});
