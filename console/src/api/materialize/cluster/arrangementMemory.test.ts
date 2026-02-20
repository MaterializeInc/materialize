// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildArrangmentMemoryUsageQuery } from "./arrangementMemory";

describe("buildArrangmentMemoryUsageQuery", () => {
  it("builds a valid query when swap is disabled", () => {
    const { sql, parameters } = buildArrangmentMemoryUsageQuery({
      arrangmentIds: ["u123"],
      replicaHeapLimit: 1024,
    }).compile();

    expect({ sql, parameters }).toMatchSnapshot();
  });
});
