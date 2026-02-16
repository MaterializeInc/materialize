// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { buildIndexesListQuery } from "./indexesList";

describe("buildIndexesListQuery", () => {
  it("build query with no filters", () => {
    const { sql, parameters } = buildIndexesListQuery({
      clusterId: "s1",
      includeSystemObjects: true,
    }).compile();

    expect({ sql, parameters }).toMatchSnapshot();
  });

  it("build query with all filters", () => {
    const { sql, parameters } = buildIndexesListQuery({
      clusterId: "u123",
      nameFilter: "source",
      databaseId: "u1",
      schemaId: "u2",
      includeSystemObjects: false,
    }).compile();

    expect({ sql, parameters }).toMatchSnapshot();
  });
});
