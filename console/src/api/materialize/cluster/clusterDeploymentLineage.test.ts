// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { expandClusterLineage } from "./clusterDeploymentLineage";

const deployment = (clusterId: string, currentDeploymentClusterId: string) => ({
  clusterId,
  currentDeploymentClusterId,
  clusterName: "prod",
});

describe("expandClusterLineage", () => {
  const pastDeploymentsByCurrentDeployment = new Map([
    ["u3", [deployment("u3", "u3"), deployment("u2", "u3")]],
  ]);

  it("includes a cluster's past blue-green deployments", () => {
    expect(
      expandClusterLineage(["u3"], pastDeploymentsByCurrentDeployment),
    ).toEqual(["u2", "u3"]);
  });

  it("keeps a cluster that has no lineage rows, like a system cluster", () => {
    expect(
      expandClusterLineage(["s2"], pastDeploymentsByCurrentDeployment),
    ).toEqual(["s2"]);
  });

  it("returns sorted, deduplicated ids regardless of input order", () => {
    const reordered = new Map([
      ["u3", [deployment("u2", "u3"), deployment("u3", "u3")]],
    ]);
    expect(expandClusterLineage(["u3", "s2", "u3"], reordered)).toEqual([
      "s2",
      "u2",
      "u3",
    ]);
  });
});
