// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  buildStatementInfoQuery,
  buildStatementLifecycleQuery,
} from "./queryHistoryDetail";

describe("buildStatementInfoQuery", () => {
  it("produces the expected query", () => {
    const { sql, parameters } = buildStatementInfoQuery({
      executionId: "25c015a3-fe6b-4943-be39-4591f8832326",
      shouldUseIndexedView: true,
    }).compile();
    expect({ sql, parameters }).toMatchSnapshot();
  });
});

describe("buildStatementLifecycleQuery", () => {
  it("produces the expected query", () => {
    const { sql, parameters } = buildStatementLifecycleQuery({
      executionId: "25c015a3-fe6b-4943-be39-4591f8832326",
    }).compile();
    expect({ sql, parameters }).toMatchSnapshot();
  });
});
