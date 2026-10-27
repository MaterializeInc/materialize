// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "../db";
import { buildCreateSecretQuery, getSecretQueryBuilder } from "./createSecrets";

describe("getSecretQueryBuilder", () => {
  it("builds the expected query", () => {
    const { parameters, sql } = getSecretQueryBuilder({
      name: "new_secret",
      databaseName: "materialize",
      schemaName: "public",
    }).compile();
    expect({ parameters, sql }).toMatchSnapshot();
  });
});

describe("buildCreateSecretQuery", () => {
  it("builds the expected query", () => {
    const { parameters, sql } = buildCreateSecretQuery({
      name: "new_secret",
      databaseName: "materialize",
      schemaName: "public",
      value: "123",
    }).compile(queryBuilder);
    expect({ parameters, sql }).toMatchSnapshot();
  });
});
