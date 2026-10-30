// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { OOM_REASON, splitReplicaStatusRows } from "./replicaStatusTimeline";

const row = (
  processId: string,
  atMs: number,
  status: string,
  reason: string | null = null,
) => ({
  replicaId: "u1",
  processId,
  occurredAt: new Date(atMs),
  status,
  reason,
});

describe("splitReplicaStatusRows", () => {
  it("takes status from process 0 and OOM kills from every process", () => {
    const rows = [
      row("0", 10, "online"),
      row("1", 20, "offline", OOM_REASON),
      row("1", 25, "online"),
      row("0", 30, "offline", OOM_REASON),
    ];

    const { transitions, oomKills } = splitReplicaStatusRows(rows);

    expect(transitions).toEqual([rows[0], rows[3]]);
    expect(oomKills).toEqual([rows[1], rows[3]]);
  });
});
