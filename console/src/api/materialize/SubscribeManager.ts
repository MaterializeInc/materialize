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
import {
  ColumnMetadata,
  ErrorCode,
  SessionVariables,
  SqlRequest,
} from "./types";
import { Connectable } from "./WebsocketConnectionManager";

export enum SUBSCRIBE_ERROR_CODE {
  CONNECTION_CLOSED = "MZC001",
  KEY_VIOLATION = "MZC002",
  INVALID_STATE = "MZC003",
}

export interface SubscribeError {
  code: ErrorCode | string;
  message: string;
}

export interface SubscribeState<T> {
  /** The current values at the most recent closed timestamp */
  data: T[];
  snapshotComplete: boolean;
  error: SubscribeError | undefined;
}

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
  httpAddress: string;
  request?: SqlRequest;
  closeSocketOnComplete?: boolean;
  sessionVariables?: SessionVariables;
  flushInterval?: number;
  upsert?: UpsertSubscribeOptions<T>;
  select?: SelectFunction<T, R>;
  /**
   * If set, every raw WebSocket message and every buffer flush is logged to
   * the console, prefixed with this label. Intended for diagnostics only.
   */
  debugLabel?: string;
}

export type SelectFunction<T extends object, R> = (row: SubscribeRow<T>) => R;

/**
 * Stateful object that executes Materialize SUBSCRIBE over a websocket. Results from the
 * socket are returned in raw form, meaning each progress message potentially causes a
 * render.
 *
 */
export class SubscribeManager<T extends object, R> implements Connectable {
  socket: MaterializeWebsocket;
  /** A function to transform the current state into the desired output when calling getSnapshot */
  select?: SelectFunction<T, R>;
  /**
   * Specifying `upsert` will ensure `data` is unique based on the
   * upsert key function. The array will be ordered by insertion.
   * The subscribe statement must include WITH (PROGRESS) and ENVELOPE UPSERT.
   */
  upsert?: UpsertSubscribeOptions<T>;
  /** The snapshot state exposed to listeners */
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
  /** Optional diagnostic label exposed so external hooks (e.g.
   * useAutomaticallyConnectSocket) can correlate logs. */
  debugLabel: string | undefined;

  constructor(options: SubscribeManagerOptions<T, R>) {
    this.debugLabel = options.debugLabel;
    if (this.debugLabel) {
      // eslint-disable-next-line no-console
      console.log(
        `[subscribe:${this.debugLabel}] constructed (hasRequest=${!!options.request})`,
      );
    }
    this.socket = new MaterializeWebsocket({
      httpAddress: options.httpAddress,
      sessionVariables: buildSessionVariables({
        ...options.sessionVariables,
      }),
      onReadyForQuery: this.onReadyForQuery,
      onMessage: this.onMessage,
      onClose: this.onClose,
      onOpen: this.onOpen,
      debugLabel: options.debugLabel,
    });
    this.upsert = options.upsert;
    this.closeSocketOnComplete = options.closeSocketOnComplete ?? false;
    this.sqlRequest = options.request;
    this.flushInterval = options.flushInterval ?? this.flushInterval;
    this.select = options.select;
    this.snapshotState = this.currentState as SubscribeState<R>;
  }

  connect = (
    request?: SqlRequest,
    httpAddress?: string,
    sessionVariables?: SessionVariables,
  ) => {
    this.disconnect();
    // Only update sqlRequest if a new request is provided (allows reconnect to reuse existing)
    if (request !== undefined) {
      this.sqlRequest = request;
    }
    this.socket.connect(httpAddress, buildSessionVariables(sessionVariables));
    this.flushIntervalHandle = setInterval(
      this.flushSocketBuffer,
      this.flushInterval,
    );
  };

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

  reset = () => {
    if (this.debugLabel) {
      // eslint-disable-next-line no-console
      console.log(
        `[subscribe:${this.debugLabel}] reset (snapshotComplete -> false)`,
      );
    }
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
    const prevSnapshotComplete = this.currentState.snapshotComplete;
    const prevDataLen = this.currentState.data.length;
    this.currentState = {
      ...this.currentState,
      ...update,
    };
    if (
      this.debugLabel &&
      prevSnapshotComplete !== this.currentState.snapshotComplete
    ) {
      // eslint-disable-next-line no-console
      console.log(
        `[subscribe:${this.debugLabel}] snapshotComplete: ${prevSnapshotComplete} -> ${this.currentState.snapshotComplete} (data.length=${this.currentState.data.length}, listeners=${this.listeners.size})`,
      );
    }
    if (
      this.debugLabel &&
      prevDataLen !== this.currentState.data.length &&
      this.currentState.snapshotComplete
    ) {
      // eslint-disable-next-line no-console
      console.log(
        `[subscribe:${this.debugLabel}] data.length: ${prevDataLen} -> ${this.currentState.data.length}`,
      );
    }
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
      if (this.debugLabel) {
        // eslint-disable-next-line no-console
        console.log(
          `[subscribe:${this.debugLabel}] onReadyForQuery: closing (closeSocketOnComplete)`,
        );
      }
      this.socket.disconnect();
      return;
    }
    if (this.sqlRequest) {
      if (this.debugLabel) {
        // eslint-disable-next-line no-console
        console.log(
          `[subscribe:${this.debugLabel}] onReadyForQuery: sending SUBSCRIBE (querySent->true)`,
        );
      }
      this.socket.send(this.sqlRequest);
      this.querySent = true;
    }
  };

  private onClose = (event: CloseEvent) => {
    if (this.debugLabel) {
      // eslint-disable-next-line no-console
      console.log(
        `[subscribe:${this.debugLabel}] onClose code=${event.code}`,
      );
    }
    this.setState({
      error: {
        code: SUBSCRIBE_ERROR_CODE.CONNECTION_CLOSED,
        message: `Socket closed unexpectedly, code: ${event.code}`,
      },
    });
  };

  private onMessage = (message: WebSocketResult) => {
    if (this.debugLabel && message.type !== "Row") {
      // Row messages get logged in detail inside onRow as state-machine steps;
      // everything else (Error, Rows-with-columns, ReadyForQuery, etc.) is logged here.
      // eslint-disable-next-line no-console
      console.log(
        `[subscribe:${this.debugLabel}] msg`,
        message.type,
        message.payload,
      );
    }
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
    if (!this.querySent) {
      if (this.debugLabel) {
        // eslint-disable-next-line no-console
        console.log(
          `[subscribe:${this.debugLabel}] onRow skipped (querySent=false)`,
        );
      }
      return;
    }

    const meta = extractSubscribeMetadata(payload, this.columns);
    const before = {
      currentTimestamp: this.currentTimestamp,
      currentTimestampType: typeof this.currentTimestamp,
      snapshotComplete: this.currentState.snapshotComplete,
    };
    const trace: string[] = [];

    if (this.currentTimestamp && meta.mzTimestamp > this.currentTimestamp) {
      // this timestamp is complete, flush it
      const updates = this.currentTimestampBuffer.get(this.currentTimestamp);
      if (updates) {
        this.closedTimestampBuffer.push(...updates);
        this.currentTimestampBuffer.delete(this.currentTimestamp);
        trace.push(
          `advance(${this.currentTimestamp}->${meta.mzTimestamp}): flushed ${updates.length} to closed`,
        );
      } else {
        trace.push(
          `advance(${this.currentTimestamp}->${meta.mzTimestamp}): no buffer for prev`,
        );
      }
    } else if (this.debugLabel && this.currentTimestamp) {
      // Helpful when timestamps are equal or compare wrong (e.g. string-compare bug):
      trace.push(
        `noadvance: cmp(${this.currentTimestamp} ${typeof this.currentTimestamp}, ${meta.mzTimestamp} ${typeof meta.mzTimestamp}) -> ${meta.mzTimestamp > this.currentTimestamp}`,
      );
    } else if (this.debugLabel) {
      trace.push(`noadvance: currentTimestamp not set`);
    }

    // Once we've received a second progress message, we know we've received
    // the initial snapshot.
    const snapshotGateChecked =
      meta.mzProgressed && !this.currentState.snapshotComplete;
    if (
      meta.mzProgressed &&
      this.currentTimestamp &&
      !this.currentState.snapshotComplete
    ) {
      // Eagerly flush the buffer to make the snapshot available.
      trace.push(`snapshotGate: FIRING (flushing+setting snapshotComplete)`);
      this.flushSocketBuffer();
      this.setState({
        snapshotComplete: true,
      });
    } else if (snapshotGateChecked) {
      // mzProgressed && !snapshotComplete but currentTimestamp falsy -> gate blocked
      trace.push(
        `snapshotGate: BLOCKED (currentTimestamp=${String(this.currentTimestamp)})`,
      );
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

    if (this.debugLabel) {
      // Only log onRow at interesting transitions, not for every progress tick:
      //  - before snapshot completes (every row is potentially relevant)
      //  - when mzState is non-null (real data: upsert/delete/key_violation)
      //  - when the snapshot-completion gate is firing or blocked
      const interesting =
        !before.snapshotComplete ||
        meta.mzState !== null ||
        trace.some((t) => t.startsWith("snapshotGate"));
      if (interesting) {
        // eslint-disable-next-line no-console
        console.log(
          `[subscribe:${this.debugLabel}] onRow`,
          {
            mzTimestamp: meta.mzTimestamp,
            mzProgressed: meta.mzProgressed,
            mzState: meta.mzState,
          },
          "before=",
          before,
          "after=",
          {
            currentTimestamp: this.currentTimestamp,
            snapshotComplete: this.currentState.snapshotComplete,
          },
          trace.length ? trace : undefined,
        );
      }
    }
  };

  private flushSocketBuffer = () => {
    if (this.closedTimestampBuffer.length === 0) return;

    if (this.debugLabel) {
      const stateCounts = this.closedTimestampBuffer.reduce<
        Record<string, number>
      >((acc, r) => {
        const k = String(r.mzState);
        acc[k] = (acc[k] ?? 0) + 1;
        return acc;
      }, {});
      const hasRealData = this.closedTimestampBuffer.some(
        (r) => r.mzState !== null,
      );
      if (hasRealData || !this.currentState.snapshotComplete) {
        // eslint-disable-next-line no-console
        console.log(
          `[subscribe:${this.debugLabel}] flush`,
          this.closedTimestampBuffer.length,
          "rows by mz_state=",
          stateCounts,
          "snapshotComplete=",
          this.currentState.snapshotComplete,
        );
      }
    }

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
        ...this.closedTimestampBuffer.values(),
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

export type SubscribeMetadata = {
  mzTimestamp: number;
  /** when mzProgressed is true, mzState is null */
  mzState: null | "upsert" | "delete" | "key_violation";
  mzProgressed?: boolean;
};

export interface SubscribeRow<T> extends SubscribeMetadata {
  data: T;
}

// Copied from https://materialize.com/docs/sql/subscribe/#output
export const SUBSCRIBE_METADATA_COLUMNS: { [columnName: string]: string } = {
  mz_timestamp: "mz_timestamp",
  mz_progressed: "mz_progressed",
  mz_diff: "mz_diff",
  mz_state: "mz_state",
};
