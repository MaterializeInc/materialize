// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import type {
  Column,
  Error,
  SessionVariables,
  SqlRequest,
} from "~/api/materialize/types";
import type { MaterializeAuthConfig } from "~/api/types";

/** Messages sent from the main thread to the execution worker. */
export type MainToWorkerMessage =
  | {
      type: "connect";
      url: string;
      authConfig: MaterializeAuthConfig | null;
      sessionVariables: SessionVariables;
    }
  | {
      type: "send";
      request: SqlRequest;
    }
  | { type: "disconnect" }
  | { type: "reconnect" };

/** Messages sent from the execution worker to the main thread. */
export type WorkerToMainMessage =
  | {
      type: "stateChange";
      readyForQuery: boolean;
      error?: string;
      isConnected: boolean;
    }
  | {
      /** Batched query result. Rows and columns are accumulated in the worker
       * and sent in a single postMessage when CommandComplete arrives. */
      type: "queryResult";
      columns: Column[];
      rows: unknown[][];
      commandComplete: string;
      /** Time from after any SET/RESET wrappers to the first Rows message
       * (time-to-first-response), measured via performance.now() in the worker. */
      durationMs: number;
    }
  | {
      type: "error";
      error: Error;
      durationMs: number;
    }
  | { type: "backendKeyData"; connId: number }
  | { type: "commandStarting"; isStreaming: boolean }
  | { type: "parameterStatus"; name: string; value: string }
  | { type: "notice"; message: string }
  | { type: "readyForQuery" }
  | {
      /** CommandComplete for SET/RESET wrapper statements. Not a query result. */
      type: "setComplete";
      tag: string;
    };
