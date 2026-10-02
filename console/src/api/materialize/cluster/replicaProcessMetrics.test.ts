// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { queryBuilder } from "~/api/materialize";

import { buildReplicaProcessMetricsSubscribe } from "./replicaProcessMetrics";

describe("buildReplicaProcessMetricsSubscribe", () => {
  it("bounds the samples by a literal start, with no temporal filter", () => {
    const { sql } = buildReplicaProcessMetricsSubscribe({
      replicaIds: ["u2"],
      since: new Date("2026-10-16T09:00:00.000Z"),
      includeSwap: false,
    }).compile(queryBuilder);

    expect(sql).toContain(
      `"m"."occurred_at" >= '2026-10-16T09:00:00.000Z'::timestamptz`,
    );
    expect(sql).not.toContain("mz_now");
  });
});
