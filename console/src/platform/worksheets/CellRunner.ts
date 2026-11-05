// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { MaterializeWebsocket } from "~/api/materialize/MaterializeWebsocket";
import { Column, SessionVariables } from "~/api/materialize/types";
import {
  ExecutionTime,
  parseExecutionTimeNotice,
} from "~/platform/shell/executionTime";

import { SubscribeDiffs } from "./subscribeDiffs";

export type RunMode = "once" | "live";

export interface RunError {
  message: string;
  detail?: string;
  hint?: string;
}

export interface CellRunState {
  status: "idle" | "connecting" | "running" | "done" | "error";
  /** The mode of the latest run, kept after it stops. */
  mode?: RunMode;
  columns: Column[];
  /** At most `MAX_DISPLAY_ROWS` rows. `totalRows` counts all of them. */
  rows: unknown[][];
  totalRows: number;
  /** For live runs, the timestamp `rows` are consistent as of. */
  frontier?: string;
  /** For live runs, whether the SUBSCRIBE ended on its own. */
  completed?: boolean;
  commandTag?: string;
  /** Statements the latest run completed. Only the last one's rows are kept. */
  statementCount: number;
  /**
   * For one-shot runs, from sending the SQL until every statement finished
   * and the rows had arrived.
   */
  roundTripMs?: number;
  /** For live runs, from sending the SQL until the first complete snapshot. */
  snapshotMs?: number;
  /** Materialize's own time for the last statement, where it reports it. */
  executionTime?: ExecutionTime;
  error?: RunError;
}

export const MAX_DISPLAY_ROWS = 1000;
const NOTIFY_INTERVAL_MS = 100;

const IDLE_STATE: CellRunState = {
  status: "idle",
  columns: [],
  rows: [],
  totalRows: 0,
  statementCount: 0,
};

/**
 * Runs one cell's statement on a dedicated websocket session, either once or
 * as `SUBSCRIBE ... WITH (PROGRESS)`.
 *
 * Each run gets its own socket because a session executes one statement at a
 * time and a live cell's SUBSCRIBE never yields the session back.
 */
export class CellRunner {
  private socket: MaterializeWebsocket | undefined;
  private snapshot: CellRunState = IDLE_STATE;
  private listeners = new Set<() => void>();
  private notifyHandle: ReturnType<typeof setTimeout> | undefined;
  /**
   * Projects the current run's buffers into the snapshot. Until a run has
   * results it projects nothing, so a re-run keeps showing the previous
   * results instead of flashing empty.
   */
  private materialize: () => Partial<CellRunState> = () => ({});

  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  getSnapshot = () => this.snapshot;

  start({
    httpAddress,
    sql,
    mode,
    sessionVariables,
  }: {
    httpAddress: string;
    sql: string;
    mode: RunMode;
    sessionVariables: SessionVariables;
  }) {
    this.stop();
    const diffs = mode === "live" ? new SubscribeDiffs() : undefined;
    let columns: Column[] = [];
    let rows: unknown[][] = [];
    let totalRows = 0;
    // Whether the statement in progress has returned a row description.
    let statementHasRows = false;
    let sent = false;
    let sentAt = 0;
    this.materialize = () => ({});

    const showResults = () => {
      this.materialize = () => {
        if (!diffs) return { columns, rows: [...rows], totalRows };
        const current = diffs.rows(MAX_DISPLAY_ROWS);
        return {
          columns: diffs.columns,
          rows: current.rows,
          totalRows: current.total,
          frontier: diffs.frontier,
        };
      };
    };

    const isCurrent = () => this.socket === socket;
    const socket = new MaterializeWebsocket({
      httpAddress,
      sessionVariables,
      onReadyForQuery: () => {
        if (!isCurrent()) return;
        if (!sent) {
          sent = true;
          sentAt = performance.now();
          // The closing parenthesis sits on its own line so a trailing `--`
          // comment in the query cannot swallow it.
          socket.send({
            query:
              mode === "live" ? `SUBSCRIBE (\n${sql}\n) WITH (PROGRESS)` : sql,
          });
          this.setState({ status: "running" });
          return;
        }
        // The server sends one ReadyForQuery per request, after every
        // statement in it has finished or one has failed.
        this.disconnect();
        if (this.snapshot.status !== "error") {
          this.setState({
            status: "done",
            ...(diffs ? {} : { roundTripMs: performance.now() - sentAt }),
          });
        }
      },
      onMessage: (message) => {
        if (!isCurrent()) return;
        switch (message.type) {
          case "Rows":
            if (diffs) {
              diffs.setColumns(message.payload.columns);
            } else {
              // Each statement of a multi-statement cell starts a new result.
              columns = message.payload.columns;
              rows = [];
              totalRows = 0;
              statementHasRows = true;
              showResults();
            }
            this.scheduleNotify();
            break;
          case "Row":
            if (diffs) {
              if (diffs.push(message.payload)) {
                showResults();
                if (this.snapshot.snapshotMs === undefined && diffs.frontier) {
                  this.setState({ snapshotMs: performance.now() - sentAt });
                }
                this.scheduleNotify();
              }
            } else {
              totalRows++;
              if (rows.length < MAX_DISPLAY_ROWS) rows.push(message.payload);
              this.scheduleNotify();
            }
            break;
          case "CommandComplete":
            if (diffs) {
              diffs.complete();
              showResults();
              this.setState({ completed: true });
              break;
            }
            if (!statementHasRows) {
              // A statement without rows (DDL, DML) replaces earlier results.
              columns = [];
              rows = [];
              totalRows = 0;
              showResults();
            }
            statementHasRows = false;
            this.setState({
              commandTag: message.payload,
              statementCount: this.snapshot.statementCount + 1,
            });
            break;
          case "Notice": {
            const executionTime = parseExecutionTimeNotice(message.payload);
            if (executionTime) this.setState({ executionTime });
            break;
          }
          case "Error":
            this.materialize = () => ({});
            this.setState({
              status: "error",
              columns: [],
              rows: [],
              totalRows: 0,
              error: {
                message: message.payload.message ?? "An error occurred",
                detail: message.payload.detail,
                hint: message.payload.hint,
              },
            });
            break;
        }
      },
      onClose: () => {
        if (!isCurrent()) return;
        this.socket = undefined;
        // An error the server reported before closing explains more than
        // the close does.
        if (this.snapshot.status === "error") return;
        this.setState({
          status: "error",
          error: { message: "Connection closed unexpectedly" },
        });
      },
    });
    this.socket = socket;
    this.setState({
      status: "connecting",
      mode,
      commandTag: undefined,
      statementCount: 0,
      roundTripMs: undefined,
      snapshotMs: undefined,
      executionTime: undefined,
      error: undefined,
      frontier: undefined,
      completed: false,
    });
    socket.connect();
  }

  /** Ends the current run, keeping its last results. */
  stop() {
    if (!this.socket) return;
    this.disconnect();
    if (
      this.snapshot.status === "running" ||
      this.snapshot.status === "connecting"
    ) {
      this.setState({ status: "done" });
    }
  }

  private disconnect() {
    // NOTE: `MaterializeWebsocket.disconnect` detaches listeners before
    // closing, so an intentional stop never reaches `onClose`.
    this.socket?.disconnect();
    this.socket = undefined;
  }

  private scheduleNotify() {
    if (this.notifyHandle !== undefined) return;
    this.notifyHandle = setTimeout(() => {
      this.notifyHandle = undefined;
      this.setState({});
    }, NOTIFY_INTERVAL_MS);
  }

  private setState(update: Partial<CellRunState>) {
    this.snapshot = { ...this.snapshot, ...this.materialize(), ...update };
    for (const listener of this.listeners) listener();
  }
}
