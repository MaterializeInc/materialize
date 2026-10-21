// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { RawBuilder } from "kysely";
import { Client } from "pg";

import { queryBuilder } from "~/api/materialize";

// Below the suite's `testTimeout`, so a snapshot that never completes fails on
// the error below rather than on the timeout.
const SUBSCRIBE_SNAPSHOT_TIMEOUT_MS = 30_000;

/**
 * A subscribe's snapshot, read through a cursor in `client`'s session. The
 * snapshot is complete at the first progress message past the first message's
 * timestamp.
 */
export async function readSubscribeSnapshot(
  client: Client,
  subscribe: RawBuilder<unknown>,
) {
  const { sql, parameters } = subscribe.compile(queryBuilder);
  const rows: Record<string, unknown>[] = [];
  await client.query("BEGIN");
  try {
    await client.query(`DECLARE c CURSOR FOR ${sql}`, [...parameters]);
    let firstTimestamp: bigint | undefined;
    const deadline = Date.now() + SUBSCRIBE_SNAPSHOT_TIMEOUT_MS;
    while (Date.now() < deadline) {
      const { rows: batch } = await client.query(
        `FETCH ALL c WITH (timeout='1s')`,
      );
      for (const row of batch) {
        const timestamp = BigInt(row.mz_timestamp);
        firstTimestamp ??= timestamp;
        if (!row.mz_progressed) rows.push(row);
        else if (timestamp > firstTimestamp) return rows;
      }
    }
    throw new Error("The subscribe's snapshot did not complete");
  } finally {
    await client.query("ROLLBACK");
  }
}
