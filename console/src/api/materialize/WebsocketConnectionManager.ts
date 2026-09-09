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

export interface ConnectionInfo {
  hasEverConnected: boolean;
}

export interface WebsocketConnectionManagerOptions {
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  getSessionVariables?: (info: ConnectionInfo) => SessionVariables | undefined;
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
  Omit<WebsocketConnectionManagerOptions, "getSessionVariables">
> &
  Pick<WebsocketConnectionManagerOptions, "getSessionVariables">;

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
  /** Address used for the most recent connection attempt. */
  private attemptedHttpAddress?: string;
  /** Region the most recent connection attempt was made for. */
  private attemptedRegionId?: string | null;
  private retryAttempt = 0;
  private hasEverConnected = false;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private initialized = false;
  /** Set while a connect is mid-handshake. Reentry would strand the CONNECTING socket on Safari. */
  private connectInFlight = false;

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

    this.isHealthy = nowHealthy;

    if (currentEnvironment?.state === "enabled") {
      this.currentHttpAddress = currentEnvironment.httpAddress;
    }

    if (nowHealthy) {
      // A connected socket may still stream from a previous region's address
      // after a region switch; reconnect at the current one.
      if (
        !this.target.isConnected() ||
        this.attemptedHttpAddress !== this.currentHttpAddress
      ) {
        this.resumeConnection();
      }
    } else {
      // Only tear down a socket pointing at a region the user has left; a
      // health blip in the current region leaves a working socket alone
      // (consumers like the Shell would get no close callback).
      const regionId = this.store.get(currentRegionIdSyncAtom);
      this.pauseConnection(regionId !== this.attemptedRegionId);
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
    this.connectInFlight = false;
    if (this.isHealthy) {
      this.scheduleRetry();
    }
    this.notifyStateChange();
  };

  private handleTargetOpen = () => {
    this.connectInFlight = false;
    this.hasEverConnected = true;
    this.retryAttempt = 0;
    this.clearRetryTimer();
    this.notifyStateChange();
    // The region may have switched while this handshake was in flight; the
    // socket that just opened points at the old address.
    if (
      this.isHealthy &&
      this.attemptedHttpAddress !== this.currentHttpAddress
    ) {
      this.attemptConnection();
    }
  };

  /** Tear down and reopen the socket. Used on SQL request changes. */
  reconnect() {
    this.attemptConnection();
  }

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
    if (this.connectInFlight) return;
    if (!this.currentHttpAddress) return;

    const sessionVariables = this.options.getSessionVariables?.({
      hasEverConnected: this.hasEverConnected,
    });
    this.connectInFlight = true;
    this.attemptedHttpAddress = this.currentHttpAddress;
    this.attemptedRegionId = this.store.get(currentRegionIdSyncAtom);
    try {
      this.target.reconnect(this.currentHttpAddress, sessionVariables);
    } catch {
      this.connectInFlight = false;
      this.scheduleRetry();
    }
  }

  private resumeConnection() {
    this.retryAttempt = 0;
    this.clearRetryTimer();
    this.attemptConnection();
  }

  private pauseConnection(disconnect: boolean) {
    this.clearRetryTimer();
    this.retryAttempt = 0;
    if (disconnect) {
      // Tear down so the resume path reconnects after an unhealthy round trip;
      // clear connectInFlight since this socket gets no open/close callback.
      this.connectInFlight = false;
      this.target.disconnect();
    }
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
