// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  buildBucketedSinkErrorsQuery,
  buildSinkErrorsQuery,
} from "./sinkErrors";

describe("sink error queries", () => {
  it("buildSinkErrorsQuery produces the expected query", () => {
    const query = buildSinkErrorsQuery({
      sinkId: "u1",
      startTime: new Date("2023-06-08T22:49:19.754Z"),
      endTime: new Date("2023-06-09T22:49:19.754Z"),
      limit: 100,
    }).compile();
    expect(query.parameters).toMatchSnapshot();
    expect(query.sql).toMatchSnapshot();
  });

  it("buildBucketedSourceErrorsQuery produces the expected query", () => {
    const query = buildBucketedSinkErrorsQuery({
      sinkId: "u1",
      startTime: new Date("2023-06-08T22:49:19.754Z"),
      endTime: new Date("2023-06-09T22:49:19.754Z"),
      bucketSizeSeconds: 180,
    }).compile();
    expect(query.parameters).toMatchSnapshot();
    expect(query.sql).toMatchSnapshot();
  });
});
