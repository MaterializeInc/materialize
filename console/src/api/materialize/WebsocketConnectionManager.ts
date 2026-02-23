// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createStore } from "jotai";

import {
  currentRegionIdSyncAtom,
  Environment,
  environmentsWithHealth,
} from "~/store/environments";

import { SessionVariables } from "./types";

export interface Connectable {
  /** Reconnect using previously stored request/config (if any) */
  reconnect(httpAddress?: string, sessionVariables?: SessionVariables): void;
  disconnect(): void;
  isConnected(): boolean;
  registerOnClose(callback: () => void): () => void;
  registerOnOpen(callback: () => void): () => void;
}

export interface WebsocketConnectionManagerOptions {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  sessionVariables?: SessionVariables;
}

const DEFAULT_OPTIONS = {
  maxAttempts: 5,
  baseDelayMs: 1000,
  maxDelayMs: 30000,
};

export type ConnectionStatus =
  | "disconnected"
  | "connected"
  | "reconnecting"
  | "failed";

export interface ReconnectionState {
  status: ConnectionStatus;
  attempt: number;
  maxAttempts: number;
  nextRetryMs: number | null;
}

type ResolvedOptions = Required<
  Omit<WebsocketConnectionManagerOptions, "sessionVariables">
> &
  Pick<WebsocketConnectionManagerOptions, "sessionVariables">;

type JotaiStore = ReturnType<typeof createStore>;

/**
 * Manages automatic reconnection for WebSocket connections with exponential backoff.
 * Subscribes directly to Jotai store for environment health state.
 */
export class WebsocketConnectionManager {
  private target: Connectable;
  private options: ResolvedOptions;
  private store: JotaiStore;
  private reconnectionStateAtom: ReturnType<
    typeof import("jotai").atom<ReconnectionState>
  >;

  private isHealthy = false;
  private currentHttpAddress?: string;
  private retryAttempt = 0;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private initialized = false;

  private unsubscribeFromClose: (() => void) | undefined;
  private unsubscribeFromOpen: (() => void) | undefined;
  private unsubscribeFromStore: (() => void) | undefined;

  constructor(
    target: Connectable,
    store: JotaiStore,
    reconnectionStateAtom: ReturnType<
      typeof import("jotai").atom<ReconnectionState>
    >,
    options?: WebsocketConnectionManagerOptions,
  ) {
    this.target = target;
    this.store = store;
    this.reconnectionStateAtom = reconnectionStateAtom;
    this.options = { ...DEFAULT_OPTIONS, ...options };

    // Subscribe to socket events
    this.unsubscribeFromClose = this.target.registerOnClose(
      this.handleTargetClose,
    );
    this.unsubscribeFromOpen = this.target.registerOnOpen(
      this.handleTargetOpen,
    );

    // Subscribe to environment state changes (both the environments map and current region)
    const unsubFromEnvs = this.store.sub(
      environmentsWithHealth,
      this.handleEnvironmentChange,
    );
    const unsubFromRegion = this.store.sub(
      currentRegionIdSyncAtom,
      this.handleEnvironmentChange,
    );
    this.unsubscribeFromStore = () => {
      unsubFromEnvs();
      unsubFromRegion();
    };

    // Process initial environment state (without triggering state notifications)
    this.handleEnvironmentChange();
    this.initialized = true;
  }

  destroy() {
    this.clearRetryTimer();
    this.unsubscribeFromClose?.();
    this.unsubscribeFromOpen?.();
    this.unsubscribeFromStore?.();
    this.target.disconnect();
  }

  private isEnvironmentHealthy(env: Environment | undefined): boolean {
    return env?.state === "enabled" && env.status.health === "healthy";
  }

  private getCurrentEnvironment(): Environment | undefined {
    const envs = this.store.get(environmentsWithHealth);
    const regionId = this.store.get(currentRegionIdSyncAtom);
    if (!envs || !regionId) return undefined;
    return envs.get(regionId);
  }

  private handleEnvironmentChange = () => {
    const currentEnvironment = this.getCurrentEnvironment();
    const nowHealthy = this.isEnvironmentHealthy(currentEnvironment);
    const prevHealthy = this.isHealthy;

    this.isHealthy = nowHealthy;

    // Update http address if environment is enabled
    if (currentEnvironment?.state === "enabled") {
      this.currentHttpAddress = currentEnvironment.httpAddress;
    }

    if (nowHealthy) {
      if (!prevHealthy || !this.target.isConnected()) {
        this.resumeConnection();
      }
    } else {
      this.pauseConnection();
    }
  };

  private computeReconnectionState(): ReconnectionState {
    let status: ConnectionStatus = "disconnected";
    if (this.target.isConnected()) {
      status = "connected";
    } else if (this.retryTimer !== undefined) {
      status = "reconnecting";
    } else if (this.retryAttempt >= this.options.maxAttempts) {
      status = "failed";
    }

    return {
      status,
      attempt: this.retryAttempt,
      maxAttempts: this.options.maxAttempts,
      nextRetryMs: this.getNextRetryDelay(),
    };
  }

  private notifyStateChange() {
    if (!this.initialized) return;
    this.store.set(this.reconnectionStateAtom, this.computeReconnectionState());
  }

  // --- Target event handlers ---

  private handleTargetClose = () => {
    if (this.isHealthy) {
      this.scheduleRetry();
    }
    this.notifyStateChange();
  };

  private handleTargetOpen = () => {
    this.retryAttempt = 0;
    this.clearRetryTimer();
    this.notifyStateChange();
  };

  // --- Retry scheduling ---

  private scheduleRetry() {
    this.clearRetryTimer();

    if (this.retryAttempt >= this.options.maxAttempts) {
      this.notifyStateChange();
      return;
    }

    const delay = this.getNextRetryDelay()!;
    this.notifyStateChange();

    this.retryTimer = setTimeout(() => {
      this.retryAttempt++;
      this.attemptConnection();
    }, delay);
  }

  private attemptConnection() {
    if (this.target.isConnected()) return;
    if (this.currentHttpAddress) {
      this.target.reconnect(
        this.currentHttpAddress,
        this.options.sessionVariables,
      );
    }
  }

  private resumeConnection() {
    this.retryAttempt = 0;
    this.clearRetryTimer();
    this.attemptConnection();
  }

  private pauseConnection() {
    this.clearRetryTimer();
    this.retryAttempt = 0;
    this.notifyStateChange();
  }

  private clearRetryTimer() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = undefined;
    }
  }

  /** Calculates delay with exponential backoff and jitter */
  private getNextRetryDelay(): number | null {
    if (!this.isHealthy) return null;
    if (this.retryAttempt >= this.options.maxAttempts) return null;

    const baseDelay = Math.min(
      this.options.baseDelayMs * Math.pow(2, this.retryAttempt),
      this.options.maxDelayMs,
    );

    // Add jitter (±25%) to prevent thundering herd
    const jitter = baseDelay * 0.25 * (Math.random() * 2 - 1);
    return Math.round(baseDelay + jitter);
  }
}
