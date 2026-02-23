// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { parseDbVersion } from "./api";

describe("parseDbVersion", () => {
  it("parses basic version string", () => {
    const version = parseDbVersion("v1.2.3 (abc123)");
    expect(version.crateVersion.major).toBe(1);
    expect(version.crateVersion.minor).toBe(2);
    expect(version.crateVersion.patch).toBe(3);
    expect(version.sha).toBe("abc123");
    expect(version.helmChartVersion).toBeUndefined();
  });

  it("parses version with helm chart", () => {
    const version = parseDbVersion("v1.2.3 (abc123, helm chart: 2.3.4)");
    expect(version.crateVersion.major).toBe(1);
    expect(version.crateVersion.minor).toBe(2);
    expect(version.crateVersion.patch).toBe(3);
    expect(version.sha).toBe("abc123");
    expect(version.helmChartVersion?.major).toBe(2);
    expect(version.helmChartVersion?.minor).toBe(3);
    expect(version.helmChartVersion?.patch).toBe(4);
  });

  it("parses version with helm chart and prerelease", () => {
    const version = parseDbVersion("v1.2.3 (abc123, helm chart: 2.3.4-dev)");
    expect(version.helmChartVersion?.major).toBe(2);
    expect(version.helmChartVersion?.minor).toBe(3);
    expect(version.helmChartVersion?.patch).toBe(4);
    expect(version.helmChartVersion?.prerelease).toEqual(["dev"]);
  });

  it("parses pre-release version", () => {
    const version = parseDbVersion("v1.2.3-dev (abc123)");
    expect(version.crateVersion.major).toBe(1);
    expect(version.crateVersion.minor).toBe(2);
    expect(version.crateVersion.patch).toBe(3);
    expect(version.crateVersion.prerelease).toEqual(["dev"]);
    expect(version.sha).toBe("abc123");
  });

  it("throws on invalid version string", () => {
    expect(() => parseDbVersion("not a version")).toThrow();
    expect(() => parseDbVersion("v1.2.3")).toThrow();
    expect(() => parseDbVersion("v1.2 (abc123)")).toThrow();
  });
});
