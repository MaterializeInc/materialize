// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// <reference lib="webworker" />

import type {
  Column,
  SessionVariables,
  WebSocketResult,
} from "~/api/materialize/types";
import type { MaterializeAuthConfig } from "~/api/types";

import type {
  MainToWorkerMessage,
  WorkerToMainMessage,
} from "./executionWorkerTypes";

declare const self: DedicatedWorkerGlobalScope;

let socket: WebSocket | undefined;
let sendTime = 0;
let durationMs = 0;
let columns: Column[] = [];
let rows: unknown[][] = [];

let lastUrl: string | undefined;
let lastAuthConfig: MaterializeAuthConfig | null = null;
let lastSessionVariables: SessionVariables = {};

function post(msg: WorkerToMainMessage) {
  self.postMessage(msg);
}

/** Freeze the query duration at the current time, if not already frozen. */
function freezeDuration() {
  if (sendTime > 0 && durationMs === 0) {
    durationMs = Math.round(performance.now() - sendTime);
  }
}

function resetQueryState() {
  sendTime = 0;
  durationMs = 0;
  columns = [];
  rows = [];
}

function disconnect() {
  if (socket) {
    socket.onopen = null;
    socket.onmessage = null;
    socket.onclose = null;
    socket.onerror = null;
    socket.close();
    socket = undefined;
  }
}

function connect(
  url: string,
  authConfig: MaterializeAuthConfig | null,
  sessionVariables: SessionVariables,
) {
  disconnect();

  lastUrl = url;
  lastAuthConfig = authConfig;
  lastSessionVariables = sessionVariables;

  socket = new WebSocket(url);

  socket.onopen = () => {
    socket?.send(
      JSON.stringify({
        options: sessionVariables,
        ...(authConfig ?? {}),
      }),
    );
  };

  socket.onmessage = (event: MessageEvent) => {
    let data: WebSocketResult;
    try {
      data = JSON.parse(event.data) as WebSocketResult;
    } catch {
      post({
        type: "error",
        error: {
          message: "Failed to parse server message",
          code: "XX000" as never,
        },
        durationMs: 0,
      });
      return;
    }

    switch (data.type) {
      case "ReadyForQuery":
        post({ type: "stateChange", readyForQuery: true, isConnected: true });
        post({ type: "readyForQuery" });
        break;

      case "BackendKeyData":
        post({ type: "backendKeyData", connId: data.payload.conn_id });
        break;

      case "CommandStarting":
        post({
          type: "commandStarting",
          isStreaming: data.payload.is_streaming,
        });
        break;

      case "Rows":
        freezeDuration();
        columns = data.payload.columns;
        break;

      case "Row":
        rows.push(data.payload);
        break;

      case "CommandComplete":
        if (data.payload === "SET" || data.payload === "RESET") {
          sendTime = performance.now();
          durationMs = 0;
          post({ type: "setComplete", tag: data.payload });
        } else {
          freezeDuration();
          post({
            type: "queryResult",
            columns,
            rows,
            commandComplete: data.payload,
            durationMs,
          });
          resetQueryState();
        }
        break;

      case "Error":
        freezeDuration();
        post({
          type: "error",
          error: data.payload,
          durationMs,
        });
        resetQueryState();
        break;

      case "ParameterStatus":
        post({
          type: "parameterStatus",
          name: data.payload.name,
          value: data.payload.value,
        });
        break;

      case "Notice":
        post({ type: "notice", message: data.payload.message });
        break;
    }
  };

  socket.onerror = () => {
    post({
      type: "stateChange",
      readyForQuery: false,
      error: "Socket error",
      isConnected: false,
    });
  };

  socket.onclose = () => {
    post({
      type: "stateChange",
      readyForQuery: false,
      error: "Connection closed unexpectedly",
      isConnected: false,
    });
  };
}

self.onmessage = (event: MessageEvent<MainToWorkerMessage>) => {
  const msg = event.data;
  switch (msg.type) {
    case "connect":
      connect(msg.url, msg.authConfig, msg.sessionVariables);
      break;

    case "send":
      if (socket && socket.readyState === WebSocket.OPEN) {
        resetQueryState();
        sendTime = performance.now();
        post({ type: "stateChange", readyForQuery: false, isConnected: true });
        socket.send(JSON.stringify(msg.request));
      } else {
        post({
          type: "error",
          error: {
            message: "WebSocket is not connected",
            code: "08003" as never,
          },
          durationMs: 0,
        });
      }
      break;

    case "disconnect":
      disconnect();
      break;

    case "reconnect":
      if (lastUrl) {
        connect(lastUrl, lastAuthConfig, lastSessionVariables);
      }
      break;
  }
};
