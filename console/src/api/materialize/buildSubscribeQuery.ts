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
  /**
   * Column name(s) that uniquely identify a row. Generates the
   * `ENVELOPE UPSERT (KEY (...))` clause so the subscribe stream sends
   * upsert/delete operations instead of raw diffs.
   *
   * Single string for a single-column key, or an array for composite keys.
   */
  upsertKey: string | string[];
  /** If set, adds `AS OF AT LEAST TIMESTAMP <date>` for temporal consistency. */
  asOfAtLeast?: Date;
}

/**
 * Wraps a SELECT query in a Materialize `SUBSCRIBE` statement with the
 * required `WITH (PROGRESS)` and `ENVELOPE UPSERT` clauses.
 *
 * The result is a Kysely `RawBuilder` that can be compiled to SQL and passed
 * to {@link useSubscribe} or {@link useGlobalUpsertSubscribe}.
 *
 * @example
 * ```ts
 * // Single-column key
 * const sub = buildSubscribeQuery(
 *   queryBuilder.selectFrom("mz_objects").select(["id", "name"]),
 *   { upsertKey: "id" },
 * );
 *
 * // Composite key
 * const sub = buildSubscribeQuery(
 *   buildAllColumnsQuery(),
 *   { upsertKey: ["objectId", "name"] },
 * );
 *
 * // Raw SQL query
 * const sub = buildSubscribeQuery(
 *   sql<MyType>`SELECT id, name FROM mz_objects WHERE id LIKE 'u%'`,
 *   { upsertKey: "id" },
 * );
 * ```
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
