// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "~/api/materialize";

import { buildStatementLoggingMaxSampleRateQuery } from "./statementLoggingMaxSampleRate";

describe("queries", () => {
  it("buildStatementLoggingMaxSampleRateQuery produces the expected query", () => {
    const { sql, parameters } =
      buildStatementLoggingMaxSampleRateQuery().compile(queryBuilder);
    expect({ sql, parameters }).toMatchSnapshot();
  });
});
