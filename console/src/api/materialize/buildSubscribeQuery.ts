// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Compilable, RawBuilder, sql } from "kysely";

export interface SubscribeQueryOptions {
  upsertKey: string | string[];
  asOfAtLeast?: Date;
}

/**
 * Builds a subscribe query given a select query and options.
 * WITH PROGRESS is always specified.
 * `upsertKey` is required, it sets ENVELOPE UPSERT with the given key.
 * `asOfAtLeast` sets AS OF AT LEAST to the specified timestamp.
 */
export function buildSubscribeQuery<T>(
  query: Compilable<T> | RawBuilder<unknown>,
  options?: SubscribeQueryOptions,
) {
  const subscribeOptions = [sql`WITH (PROGRESS)`];
  if (options?.asOfAtLeast) {
    subscribeOptions.push(
      sql`AS OF AT LEAST TIMESTAMP ${sql.lit(options.asOfAtLeast.toISOString())}`,
    );
  }
  if (options?.upsertKey) {
    const key =
      typeof options.upsertKey === "string"
        ? [options.upsertKey]
        : options.upsertKey;
    subscribeOptions.push(
      sql`ENVELOPE UPSERT (KEY (${sql.join(key.map((k) => sql.ref(k)))}))`,
    );
  }
  return sql<T>`SUBSCRIBE (${query}) ${sql.join(subscribeOptions, sql` `)};`;
}
