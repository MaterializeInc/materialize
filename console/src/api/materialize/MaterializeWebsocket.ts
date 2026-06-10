// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: This class should not depend on the apiClient singleton for better modularity and testability
import { apiClient } from "~/api/apiClient";

import type { Column, Error, Notice, SessionVariables } from "./types";
import { SqlRequest } from "./types";
import { Connectable } from "./WebsocketConnectionManager";

export type Callback = () => void;
export type MessageCallback = (message: WebSocketResult) => void;
export type CloseCallback = (event: CloseEvent) => void;
export type OpenCallback = (event: Event) => void;

export interface MaterializeWebsocketState {
  readyForQuery: boolean;
  error: string | undefined;
}

export class MaterializeWebsocket implements Connectable {
  httpAddress: string;
  sessionVariables: SessionVariables;
  private socket: WebSocket | undefined;
  private onReadyForQuery: Callback | undefined;
  private listeners = new Set<() => void>();
  private closeListeners = new Set<() => void>();
  private openListeners = new Set<() => void>();
  private currentState: MaterializeWebsocketState = {
    readyForQuery: false,
    error: undefined,
  };
  onMessage: MessageCallback | undefined;
  onClose: CloseCallback | undefined;
  onOpen: OpenCallback | undefined;

  constructor(options: {
    httpAddress: string;
    sessionVariables?: SessionVariables;
    onReadyForQuery?: Callback;
    onMessage?: MessageCallback;
    onClose?: CloseCallback;
    onOpen?: OpenCallback;
  }) {
    this.httpAddress = options.httpAddress;
    this.sessionVariables = options.sessionVariables ?? {};
    this.onReadyForQuery = options.onReadyForQuery;
    this.onMessage = options.onMessage;
    this.onClose = options.onClose;
    this.onOpen = options.onOpen;
  }

  connect(httpAddress?: string, sessionVariables?: SessionVariables) {
    this.httpAddress = httpAddress ?? this.httpAddress;
    this.sessionVariables = sessionVariables ?? this.sessionVariables;
    this.disconnect();
    this.socket = new WebSocket(
      `${apiClient.mzWebsocketUrlScheme}://${this.httpAddress}/api/experimental/sql`,
    );
    this.socket.addEventListener("open", this.handleOpen);
    this.socket.addEventListener("message", this.handleMessage);
    this.socket.addEventListener("close", this.handleClose);
    this.socket.addEventListener("error", this.handleError);
  }

  disconnect() {
    this.setState({
      readyForQuery: false,
      error: undefined,
    });
    this.socket?.removeEventListener("open", this.handleOpen);
    this.socket?.removeEventListener("message", this.handleMessage);
    this.socket?.removeEventListener("close", this.handleClose);
    this.socket?.removeEventListener("error", this.handleError);
    this.socket?.close();
  }

  send(request: SqlRequest) {
    if (this.socket) {
      this.setState({
        readyForQuery: false,
      });
      this.socket.send(JSON.stringify(request));
    }
  }

  get isInitialized() {
    return Boolean(this.socket);
  }

  get isReadyForQuery() {
    return this.currentState.readyForQuery;
  }

  get error() {
    return this.currentState.error;
  }

  /**
   * Returns true if the WebSocket is currently open.
   * Used by WebsocketConnectionManager to check connection status.
   */
  isConnected() {
    return this.socket?.readyState === WebSocket.OPEN;
  }

  /**
   * Register a callback to be notified when the socket closes.
   * Returns an unsubscribe function.
   * Used by WebsocketConnectionManager to trigger reconnection.
   */
  registerOnClose(callback: () => void) {
    this.closeListeners.add(callback);
    return () => this.closeListeners.delete(callback);
  }

  /**
   * Register a callback to be notified when the socket opens.
   * Returns an unsubscribe function.
   * Used by WebsocketConnectionManager to update state on successful connection.
   */
  registerOnOpen(callback: () => void) {
    this.openListeners.add(callback);
    return () => this.openListeners.delete(callback);
  }

  /**
   * Reconnect with the given address and session variables.
   * For MaterializeWebsocket, this is equivalent to connect().
   * Used by WebsocketConnectionManager for automatic reconnection.
   */
  reconnect(httpAddress?: string, sessionVariables?: SessionVariables) {
    this.connect(httpAddress, sessionVariables);
  }

  onChange = (callback: () => void) => {
    this.listeners.add(callback);
    return () => this.listeners.delete(callback);
  };

  getSnapshot = () => {
    return this.currentState;
  };

  private setState(update: Partial<MaterializeWebsocketState>) {
    this.currentState = {
      ...this.currentState,
      ...update,
    };
    for (const callback of this.listeners) {
      callback();
    }
  }

  private handleOpen = (event: Event) => {
    this.onOpen?.(event);
    // Notify open listeners (used by WebsocketConnectionManager to update state)
    for (const callback of this.openListeners) {
      callback();
    }
    if (this.socket) {
      const wsAuthConfig = apiClient.getWsAuthConfig();
      this.socket.send(
        JSON.stringify({
          options: this.sessionVariables,
          ...(wsAuthConfig ?? {}),
        }),
      );
    }
  };

  private handleMessage = (event: MessageEvent) => {
    const data = JSON.parse(event.data) as WebSocketResult;
    if (data.type === "ReadyForQuery") {
      this.setState({
        readyForQuery: true,
      });
      this.onReadyForQuery?.();
    }
    this.onMessage?.(data);
  };

  private handleError = () => {
    this.setState({
      error: "Socket error",
    });
    for (const callback of this.listeners) {
      callback();
    }
  };

  private handleClose = (event: CloseEvent) => {
    this.setState({
      readyForQuery: false,
      error: "Connection closed unexpectedly",
    });
    for (const callback of this.listeners) {
      callback();
    }
    // Notify close listeners (used by WebsocketConnectionManager for reconnection)
    for (const callback of this.closeListeners) {
      callback();
    }
    this.onClose?.(event);
  };
}

export interface ParameterStatus {
  name: string;
  value: string;
}

export interface CommandStarting {
  has_rows: boolean;
  is_streaming: boolean;
}

export interface BackendKeyData {
  conn_id: number;
  secret_key: number;
}

export type WebsocketRow = { type: "Row"; payload: unknown[] };

export type WebSocketResult =
  | { type: "ReadyForQuery"; payload: string }
  | { type: "Notice"; payload: Notice }
  | { type: "CommandComplete"; payload: string }
  | { type: "Error"; payload: Error }
  | { type: "Rows"; payload: { columns: Column[] } }
  | { type: "Row"; payload: unknown[] }
  | { type: "ParameterStatus"; payload: ParameterStatus }
  | { type: "CommandStarting"; payload: CommandStarting }
  | { type: "BackendKeyData"; payload: BackendKeyData };
