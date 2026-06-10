// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildClusterReplicaMetricsQuery } from "./clusterReplicaMetrics";

describe("fetchClusterReplicaMetrics", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime("1999-09-03");
  });

  it("should throw if start date is after end date in dateRange", () => {
    const { sql, parameters } = buildClusterReplicaMetricsQuery({
      clusterId: "u1",
    }).compile();

    expect({ sql, parameters }).toMatchSnapshot();
  });
});
