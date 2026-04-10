// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { captureException } from "@sentry/react";

import { snakeToCamelCase } from "~/util";

import {
  buildSessionVariables,
  convertResultValue,
  mapColumnToColumnMetadata,
  mapRowToObject,
} from ".";
import { MaterializeWebsocket, WebSocketResult } from "./MaterializeWebsocket";
import { ColumnMetadata, SessionVariables, SqlRequest } from "./types";
import { Connectable } from "./WebsocketConnectionManager";

/** Application-level error codes for subscribe failures (not from the server). */
export enum SUBSCRIBE_ERROR_CODE {
  /** The WebSocket closed unexpectedly (network drop, server restart). */
  CONNECTION_CLOSED = "MZC001",
  /** An ENVELOPE UPSERT key violation was received — indicates a query or key function bug. */
  KEY_VIOLATION = "MZC002",
  /** An unrecognized `mz_state` value was received. */
  INVALID_STATE = "MZC003",
}

export interface SubscribeError {
  /** Server error code or one of {@link SUBSCRIBE_ERROR_CODE}. */
  code: string;
  message: string;
}

/**
 * The external state exposed to React components via `getSnapshot()`.
 *
 * - `data` is the **current materialized view** of all rows at the latest
 *   closed timestamp, after upsert/delete processing.
 * - `snapshotComplete` flips to `true` once the initial snapshot has been
 *   fully received (after the second progress message).
 * - `error` is set if the connection drops or a protocol error occurs.
 *   It is cleared on reconnect — the `onOpen` handler calls `reset()`
 *   which restores a fresh empty state before the new snapshot arrives.
 */
export interface SubscribeState<T> {
  data: T[];
  snapshotComplete: boolean;
  error: SubscribeError | undefined;
}

/**
 * Extracts a unique string key from a subscribe row. Used with ENVELOPE UPSERT
 * to maintain a deduplicated map of rows. The key must be stable for a given
 * logical row — if a row is updated, the new version must produce the same key
 * so the old version is replaced.
 *
 * @example
 * ```ts
 * // Single column key
 * const upsertKey = (row) => row.data.id;
 *
 * // Composite key
 * const upsertKey = (row) => `${row.data.objectId}:${row.data.name}`;
 * ```
 */
export interface UpsertKeyFunction<T> {
  (row: SubscribeRow<T>): string;
}

export interface UpsertSubscribeOptions<T> {
  key: UpsertKeyFunction<T>;
}

export interface SubscribeManagerOptions<
  T extends object,
  R = SubscribeRow<T>,
> {
  /** WebSocket endpoint (e.g. the environment's httpAddress). */
  httpAddress: string;
  /** The SQL request to send once the connection is ready. Can be omitted and provided later via `connect()`. */
  request?: SqlRequest;
  /** If true, disconnects the socket after the query completes (useful for one-shot queries). */
  closeSocketOnComplete?: boolean;
  /** Session variables (cluster, database, search_path) sent on connection. */
  sessionVariables?: SessionVariables;
  /**
   * How often (in ms) to flush buffered updates to state and notify listeners.
   * Lower values = more responsive but more renders. Default: 16ms (~1 frame).
   */
  flushInterval?: number;
  /** Enables ENVELOPE UPSERT deduplication. Required when the SUBSCRIBE uses `ENVELOPE UPSERT`. */
  upsert?: UpsertSubscribeOptions<T>;
  /**
   * Transforms each `SubscribeRow<T>` into the shape `R` exposed in `data[]`.
   * Commonly used to strip metadata: `select: (row) => row.data`.
   * Applied lazily during `setState`, not on every incoming message.
   */
  select?: SelectFunction<T, R>;
}

/** Transforms a raw `SubscribeRow<T>` into the consumer-facing shape `R`. */
export type SelectFunction<T extends object, R> = (row: SubscribeRow<T>) => R;

/**
 * Manages a single Materialize SUBSCRIBE query over a WebSocket connection.
 *
 * ## What it does
 *
 * Connects to a Materialize environment via WebSocket, sends a SUBSCRIBE query,
 * and maintains a **live materialized view** of the results. Incoming rows are
 * buffered per-timestamp and flushed to state when each timestamp closes
 * (indicated by a progress message). This avoids per-row re-renders.
 *
 * When configured with `upsert`, it maintains a deduplicated `Map` of rows
 * keyed by the `upsertKey` function. Upserts replace existing rows; deletes
 * remove them. The `data` array exposed via `getSnapshot()` is the current
 * set of live rows.
 *
 * ## How to use it
 *
 * You typically don't use `SubscribeManager` directly. Instead, use one of
 * the React hooks that wrap it:
 *
 * - **`useSubscribe()`** — per-component subscribe with local state. Opens a
 *   WebSocket on mount, closes on unmount. Good for ephemeral data.
 *
 * - **`useGlobalUpsertSubscribe()`** — app-wide subscribe with state in a Jotai
 *   atom. Started once in `AppInitializer`, data shared across all components.
 *   Used for catalog data (objects, columns, indexes, dependencies).
 *
 * Both hooks wire up `WebsocketConnectionManager` for automatic reconnection
 * with exponential backoff if the network drops.
 *
 * ## Lifecycle
 *
 * ```
 * connect() → WebSocket opens → onOpen() resets state
 *          → server sends ReadyForQuery → we send the SUBSCRIBE SQL
 *          → server streams Row messages, buffered per-timestamp
 *          → progress message closes a timestamp → buffer flushed to state
 *          → 2nd progress message → snapshotComplete = true
 *          → continues streaming incremental updates indefinitely
 *
 * Network drop → onClose() sets error → WebsocketConnectionManager
 *              → schedules retry with exponential backoff (1s, 2s, 4s...)
 *              → reconnect() → connect() with same SQL request
 *              → onOpen() resets state → fresh snapshot replays
 * ```
 *
 * ## React integration
 *
 * Uses the external store pattern (`onChange` + `getSnapshot`) compatible with
 * `React.useSyncExternalStore`. The `snapshotState` reference is replaced (not
 * mutated) on every state change so React detects updates.
 *
 * @see useSubscribe — per-component hook
 * @see useGlobalUpsertSubscribe — global hook with Jotai atom
 * @see WebsocketConnectionManager — automatic reconnection
 * @see buildSubscribeQuery — constructs SUBSCRIBE SQL with WITH (PROGRESS) and ENVELOPE UPSERT
 */
export class SubscribeManager<T extends object, R> implements Connectable {
  socket: MaterializeWebsocket;
  /** Transforms raw rows into the consumer-facing shape. See {@link SelectFunction}. */
  select?: SelectFunction<T, R>;
  /** When set, rows are deduplicated by key. The SUBSCRIBE must use `ENVELOPE UPSERT`. */
  upsert?: UpsertSubscribeOptions<T>;
  /**
   * The current state exposed to React via `getSnapshot()`.
   * Replaced (not mutated) on every update so `useSyncExternalStore` detects changes.
   */
  snapshotState: SubscribeState<R>;
  private sqlRequest: SqlRequest | undefined;
  private listeners = new Set<() => void>();
  private columns: ColumnMetadata[] = [];
  private closeSocketOnComplete: boolean = false;
  private querySent: boolean = false;
  private currentTimestamp: number | undefined;
  /** Holds completed timestamp messages, waiting for the flush interval */
  private closedTimestampBuffer: SubscribeRow<T>[] = [];
  /** Holds messages from the socket until the timestamp is closed */
  private currentTimestampBuffer = new Map<number, Array<SubscribeRow<T>>>();
  /** The current state used internally */
  private currentState: SubscribeState<SubscribeRow<T>> = {
    data: [],
    snapshotComplete: false,
    error: undefined,
  };

  private flushIntervalHandle: NodeJS.Timeout | undefined;
  private flushInterval: number = 16;

  constructor(options: SubscribeManagerOptions<T, R>) {
    this.socket = new MaterializeWebsocket({
      httpAddress: options.httpAddress,
      sessionVariables: buildSessionVariables({
        ...options.sessionVariables,
      }),
      onReadyForQuery: this.onReadyForQuery,
      onMessage: this.onMessage,
      onClose: this.onClose,
      onOpen: this.onOpen,
    });
    this.upsert = options.upsert;
    this.closeSocketOnComplete = options.closeSocketOnComplete ?? false;
    this.sqlRequest = options.request;
    this.flushInterval = options.flushInterval ?? this.flushInterval;
    this.select = options.select;
    this.snapshotState = this.currentState as SubscribeState<R>;
  }

  /**
   * Opens the WebSocket and begins streaming. If `request` is provided, it
   * replaces the stored SQL request; otherwise the previously stored request
   * is reused (enabling reconnection without re-specifying the query).
   *
   * Calling `connect()` on an already-connected manager disconnects first.
   */
  connect = (
    request?: SqlRequest,
    httpAddress?: string,
    sessionVariables?: SessionVariables,
  ) => {
    this.disconnect();
    if (request !== undefined) {
      this.sqlRequest = request;
    }
    this.socket.connect(httpAddress, buildSessionVariables(sessionVariables));
    this.flushIntervalHandle = setInterval(
      this.flushSocketBuffer,
      this.flushInterval,
    );
  };

  /** Closes the WebSocket and stops the flush interval. State is preserved. */
  disconnect = () => {
    clearInterval(this.flushIntervalHandle);
    this.socket.disconnect();
  };

  /**
   * Returns true if the underlying socket is connected.
   * Used by WebsocketConnectionManager to check connection status.
   */
  isConnected() {
    return this.socket.isConnected();
  }

  /**
   * Register a callback to be notified when the socket closes.
   * Returns an unsubscribe function.
   * Used by WebsocketConnectionManager to trigger reconnection.
   */
  registerOnClose(callback: () => void) {
    return this.socket.registerOnClose(callback);
  }

  /**
   * Register a callback to be notified when the socket opens.
   * Returns an unsubscribe function.
   * Used by WebsocketConnectionManager to update state on successful connection.
   */
  registerOnOpen(callback: () => void) {
    return this.socket.registerOnOpen(callback);
  }

  /**
   * Reconnect using the previously stored SQL request.
   * Used by WebsocketConnectionManager for automatic reconnection.
   */
  reconnect(httpAddress?: string, sessionVariables?: SessionVariables) {
    // Pass undefined for request to reuse the existing sqlRequest
    this.connect(undefined, httpAddress, sessionVariables);
  }

  /** Clears all buffered and materialized state. Called automatically by `onOpen` after a reconnect. */
  reset = () => {
    this.columns = [];
    this.querySent = false;
    this.closedTimestampBuffer = [];
    this.currentTimestamp = undefined;
    this.currentTimestampBuffer = new Map();
    this.setState({
      data: [],
      snapshotComplete: false,
      error: undefined,
    });
  };

  onOpen = () => {
    // We wait until the socket opens successfully to reset state so that we can continue
    // to show stale data if the socket is closed unexpectedly.
    this.reset();
  };

  /**
   * Registers a listener called whenever state changes. Returns an unsubscribe function.
   * Used internally by `useSyncExternalStore` — you typically don't call this directly.
   */
  onChange = (callback: () => void) => {
    this.listeners.add(callback);
    return () => {
      this.listeners.delete(callback);
    };
  };

  /**
   * Returns the current state of the subscribe. The name refers to the React concept,
   * not the Materialize one.
   */
  getSnapshot = () => {
    return this.snapshotState;
  };

  private setState(update: Partial<SubscribeState<SubscribeRow<T>>>) {
    this.currentState = {
      ...this.currentState,
      ...update,
    };
    // We need to have the snapshot state as a separate object and not just a derived copy of this.currentState because
    // React.useSyncExternalStore requires a stable reference of the snapshot state, otherwise it will enter an infinite render loop.
    this.snapshotState = {
      ...this.currentState,
      data: (this.select
        ? this.currentState.data.map(this.select)
        : this.currentState.data) as R[],
    };
    for (const callback of this.listeners) {
      callback();
    }
  }

  private onReadyForQuery = () => {
    if (this.querySent && this.closeSocketOnComplete) {
      this.socket.disconnect();
      return;
    }
    if (this.sqlRequest) {
      this.socket.send(this.sqlRequest);
      this.querySent = true;
    }
  };

  private onClose = (event: CloseEvent) => {
    this.setState({
      error: {
        code: SUBSCRIBE_ERROR_CODE.CONNECTION_CLOSED,
        message: `Socket closed unexpectedly, code: ${event.code}`,
      },
    });
  };

  private onMessage = (message: WebSocketResult) => {
    if (message.type === "Error") {
      captureException(
        new Error(`Subscribe error: ${JSON.stringify(message.payload)}`),
      );
      this.setState({
        error: {
          code: message.payload.code,
          message: message.payload.message ?? "Unknown error",
        },
      });
      return;
    }
    if (message.type === "Rows") {
      this.columns = message.payload.columns.map(mapColumnToColumnMetadata);
    }
    if (message.type === "Row") {
      this.onRow(message.payload);
    }
  };

  private onRow = (payload: unknown[]) => {
    // If querySent is false, it means we are still getting results from a previous
    // query, ignore the data.
    if (!this.querySent) return;

    const meta = extractSubscribeMetadata(payload, this.columns);
    if (this.currentTimestamp && meta.mzTimestamp > this.currentTimestamp) {
      // this timestamp is complete, flush it
      const updates = this.currentTimestampBuffer.get(this.currentTimestamp);
      if (updates) {
        this.closedTimestampBuffer.push(...updates);
        this.currentTimestampBuffer.delete(this.currentTimestamp);
      }
    }

    // Once we've received a second progress message, we know we've received
    // the initial snapshot.
    if (
      meta.mzProgressed &&
      this.currentTimestamp &&
      !this.currentState.snapshotComplete
    ) {
      // Eagerly flush the buffer to make the snapshot available.
      this.flushSocketBuffer();
      this.setState({
        snapshotComplete: true,
      });
    }
    // Track the new currently open timestamp.
    this.currentTimestamp = meta.mzTimestamp;
    const row = mapRowToObject<T>(payload, this.columns, [
      "mz_timestamp",
      "mz_state",
      "mz_progressed",
    ]);
    const updates = this.currentTimestampBuffer.get(meta.mzTimestamp) ?? [];
    updates.push({
      ...meta,
      data: row,
    });
    this.currentTimestampBuffer.set(meta.mzTimestamp, updates);
  };

  private flushSocketBuffer = () => {
    if (this.closedTimestampBuffer.length === 0) return;

    const clearBuffer = () => {
      this.closedTimestampBuffer = [];
    };

    const newState = {
      ...this.currentState,
    };

    if (this.upsert) {
      const { key: upsertKeySelector } = this.upsert;

      // Convert whatever the current state is into an upsert map based on the upsert key function
      const upsertMap = new Map<string, SubscribeRow<T>>(
        this.currentState.data.map((row) => [upsertKeySelector(row), row]),
      );

      // We want to filter out progress messages which have mzState === null
      const filteredChanges = this.closedTimestampBuffer.filter(
        (change) => change.mzState !== null,
      );

      // If there are no real changes, we can clear the buffer and return early to avoid
      // unnecessary notifications to listeners.
      if (filteredChanges.length === 0) {
        clearBuffer();
        return;
      }

      for (const change of filteredChanges) {
        switch (change.mzState) {
          case "upsert":
            upsertMap.set(upsertKeySelector(change), change);
            break;
          case "delete":
            upsertMap.delete(upsertKeySelector(change));
            break;
          case "key_violation":
            captureException(
              new Error(`Invalid mz_state: ${JSON.stringify(change)}`),
            );
            newState.error = {
              code: SUBSCRIBE_ERROR_CODE.KEY_VIOLATION,
              message: "Key violation",
            };
            break;
          default:
            captureException(
              new Error(`Invalid mz_state: ${JSON.stringify(change)}`),
            );
            newState.error = {
              code: SUBSCRIBE_ERROR_CODE.INVALID_STATE,
              message: "Internal error",
            };
        }
      }
      // Convert the upsert map back into an array once upsert operation is complete.
      newState.data = Array.from(upsertMap.values());
    } else {
      newState.data = [
        ...newState.data,
        ...Array.from(this.closedTimestampBuffer.values()).filter(
          (row) => row.mzState !== null,
        ),
      ];
    }

    this.setState(newState);
    clearBuffer();
  };
}

export function extractSubscribeMetadata(
  row: unknown[],
  columns: ColumnMetadata[],
) {
  const result: Record<string, unknown> = {};
  for (let i = 0; i < columns.length; i++) {
    const col = columns[i];
    if (SUBSCRIBE_METADATA_COLUMNS[col.name]) {
      result[snakeToCamelCase(col.name)] = convertResultValue(row[i], col);
    }
  }
  return result as SubscribeMetadata;
}

/**
 * Metadata columns injected by Materialize into every SUBSCRIBE output row.
 * These are stripped from the user-facing `data` field and tracked separately.
 */
export type SubscribeMetadata = {
  /** The logical timestamp of this row. Rows with the same timestamp form an atomic batch. */
  mzTimestamp: number;
  /**
   * The operation type for ENVELOPE UPSERT subscribes:
   * - `"upsert"` — insert or update this row
   * - `"delete"` — remove the row with this key
   * - `"key_violation"` — error: duplicate key (should not happen in practice)
   * - `null` — this is a progress message, not a data row
   */
  mzState: null | "upsert" | "delete" | "key_violation";
  /** True on progress messages that mark timestamp boundaries. */
  mzProgressed?: boolean;
};

/**
 * A single row from a SUBSCRIBE stream, combining the user data (`T`) with
 * Materialize metadata (timestamp, state, progress). The `select` function
 * typically extracts just `row.data` to expose to consumers.
 */
export interface SubscribeRow<T> extends SubscribeMetadata {
  data: T;
}

/** Column names that Materialize adds to SUBSCRIBE output. See https://materialize.com/docs/sql/subscribe/#output */
export const SUBSCRIBE_METADATA_COLUMNS: { [columnName: string]: string } = {
  mz_timestamp: "mz_timestamp",
  mz_progressed: "mz_progressed",
  mz_diff: "mz_diff",
  mz_state: "mz_state",
};
