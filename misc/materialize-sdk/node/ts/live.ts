// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import { Subscription } from "#core";
import {
  Client,
  DatabaseError,
  type ClientConfig,
  type QueryArrayConfig,
} from "pg";

import { ServerError, UsageError } from "./errors.js";
import type { Event } from "./values.js";
import { eventFromJson } from "./values_json.js";

/** One row's cells in their text format, `null` for SQL `NULL`. */
type Row = (string | null)[];

export interface LiveOptions {
  maxRows?: number;
  fetchTimeoutMs?: number;
  /** `null` turns the limit off. */
  maxPendingBytes?: number | null;
  /** At most `MAX_TIMESTAMP_BYTES`, the default. */
  maxTimestampBytes?: number;
}

/**
 * The largest `maxTimestampBytes`. A timestamp's updates cross the binding as
 * one JSON string, and V8 refuses a string over 2^29 - 24 characters, so a
 * limit near that loses the batch with a plain `Error`. The limit counts the
 * core's estimate, which escaped text in the JSON can exceed.
 */
// TODO: hand events across one at a time, which removes this cap.
export const MAX_TIMESTAMP_BYTES = 256 * 2 ** 20;

/**
 * A snapshot, then every change, as events, with each timestamp and diff as a
 * `bigint`. The subscription owns its connection, and `close`, or leaving a
 * `for await` loop, ends the session and the subscribe on the server.
 */
export class LiveSubscription implements AsyncIterableIterator<Event> {
  private readonly events: Event[] = [];
  private failed: unknown;
  private done = false;
  /** The error the connection itself reported, which explains the failure of
   * every request after it. */
  private lost: unknown;
  private closed = false;
  /** The latest `next`, which the following one waits for. */
  private reading: Promise<unknown> = Promise.resolve();

  private constructor(
    private readonly client: Client,
    private readonly subscription: Subscription,
    private readonly fetchInput: string,
  ) {}

  static async open(
    connection: string | ClientConfig,
    query: string,
    options: LiveOptions = {},
  ): Promise<LiveSubscription> {
    const {
      maxRows = 1000,
      fetchTimeoutMs: timeout = 1000,
      maxPendingBytes = 64 * 2 ** 20,
      maxTimestampBytes = MAX_TIMESTAMP_BYTES,
    } = options;
    // Checked here, so that an invalid option fails before connecting.
    if (!Number.isSafeInteger(maxRows) || maxRows < 1) {
      throw new UsageError(`maxRows is a positive integer, not ${maxRows}`);
    }
    if (!Number.isFinite(timeout) || timeout < 0) {
      throw new UsageError(`fetchTimeoutMs is at least 0, not ${timeout}`);
    }
    const fetchTimeoutMs = Math.round(timeout);
    if (!(maxTimestampBytes <= MAX_TIMESTAMP_BYTES)) {
      throw new UsageError(
        `maxTimestampBytes is at most ${MAX_TIMESTAMP_BYTES}, not ${maxTimestampBytes}`,
      );
    }
    const subscription = new Subscription(
      JSON.stringify({
        subscribe: {
          target: { kind: "query", sql: query },
          envelope: { kind: "diff" },
        },
        start: { kind: "snapshot" },
        max_pending_bytes: maxPendingBytes?.toString(),
        max_timestamp_bytes: maxTimestampBytes.toString(),
      }),
    );
    const fetchInput = JSON.stringify({
      max_rows: maxRows.toString(),
      timeout_ms: fetchTimeoutMs.toString(),
    });
    const client = new Client({
      ...(typeof connection === "string"
        ? { connectionString: connection }
        : connection),
      // `end` waits for pipelined queries to drain, so it could not stop an
      // untimed FETCH on a quiet stream.
      pipeline: false,
    });
    // NOTE: `pg.defaults.binary` would turn on binary results for every query,
    // and the core decodes only the text format. The client is the
    // subscription's own, so overriding it changes nothing else.
    (client as unknown as { binary: boolean }).binary = false;
    const live = new LiveSubscription(client, subscription, fetchInput);
    // NOTE: a client without an `error` listener crashes the process when its
    // connection drops while idle. The server can also end the session with
    // an error between requests, such as an expired authentication, which
    // only this listener receives.
    client.on("error", (error) => {
      live.lost ??= error;
    });
    try {
      await client.connect().catch((error: unknown) => {
        throw live.failure(error);
      });
      await live.run("BEGIN", "BEGIN");
      await live.run(subscription.declareSql(), "DECLARE");
    } catch (error) {
      await client.end().catch(() => {});
      throw error;
    }
    return live;
  }

  [Symbol.asyncIterator](): LiveSubscription {
    return this;
  }

  next(): Promise<IteratorResult<Event, undefined>> {
    // A call that overlapped the one before would find its FETCH outstanding.
    const next = this.reading.then(() => this.step());
    this.reading = next.catch(() => {});
    return next;
  }

  private async step(): Promise<IteratorResult<Event, undefined>> {
    if (this.failed !== undefined) {
      throw this.failed;
    }
    if (this.done) {
      return { done: true, value: undefined };
    }
    let result: IteratorResult<Event, undefined>;
    try {
      result = await this.nextEvent();
    } catch (error) {
      // The binding still waits for the failed FETCH, so every later call
      // throws the first failure again. `for await` does not call `return`
      // after a failure, so the connection closes here.
      this.failed = error;
      await this.close().catch(() => {});
      throw error;
    }
    if (result.done) {
      this.done = true;
      await this.close();
    }
    return result;
  }

  async return(): Promise<IteratorResult<Event, undefined>> {
    await this.close();
    return { done: true, value: undefined };
  }

  /** The stream's `AS OF`, once known. */
  get asOf(): bigint | undefined {
    return this.subscription.asOf;
  }

  /** Closes the connection, which ends the subscribe on the server. */
  async close(): Promise<void> {
    this.done = true;
    if (!this.closed) {
      this.closed = true;
      await this.client.end();
    }
  }

  private async nextEvent(): Promise<IteratorResult<Event, undefined>> {
    while (this.events.length === 0) {
      if (this.subscription.isFinished) {
        return { done: true, value: undefined };
      }
      const fetch = JSON.parse(this.subscription.fetch(this.fetchInput));
      const pushed = JSON.parse(
        this.subscription.push(
          JSON.stringify(await this.run(fetch.sql, "FETCH")),
        ),
      );
      // One push per event: a FETCH can release more events than a call
      // takes arguments.
      for (const event of pushed.events) {
        this.events.push(eventFromJson(event));
      }
    }
    return { done: false, value: this.events.shift()! };
  }

  /** Runs `sql`, which completes with `command`, and returns its columns and
   * rows, or throws a `ServerError` with the core's classification. */
  private async run(
    sql: string,
    command: string,
  ): Promise<{ columns: string[]; rows: Row[] }> {
    const query: QueryArrayConfig & { queryMode: "extended" } = {
      text: sql,
      rowMode: "array",
      // The extended protocol refuses text that holds more than one
      // statement, which a query could otherwise smuggle into the DECLARE.
      queryMode: "extended",
      // Every cell stays in its text format, the form the core decodes.
      types: { getTypeParser: () => (value: string) => value },
    };
    let result;
    try {
      result = await this.client.query<Row>(query);
    } catch (error) {
      throw this.failure(error);
    }
    // Anything else could pass for an empty FETCH, which ends the stream.
    if (result.command !== command) {
      throw this.failure(
        new Error(
          `expected ${command}, the server completed ${result.command}`,
        ),
      );
    }
    return {
      columns: result.fields.map((field) => field.name),
      rows: result.rows,
    };
  }

  private failure(failed: unknown): ServerError {
    const error = this.lost ?? failed;
    // A host with several addresses fails with an AggregateError, whose own
    // message is empty.
    const message =
      error instanceof AggregateError
        ? error.errors
            .map((inner) => String(inner?.message ?? inner))
            .join("; ")
        : error instanceof Error
          ? error.message
          : String(error);
    // Only a server error carries a SQLSTATE. A socket error's `code` is not one.
    const sqlstate = error instanceof DatabaseError ? error.code : undefined;
    const classification = JSON.parse(
      this.subscription.classify(JSON.stringify({ sqlstate, message })),
    );
    return new ServerError(message, sqlstate, classification);
  }
}
