// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom } from "jotai";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { getStore } from "~/jotai";
import {
  EnvironmentsWithHealth,
  environmentsWithHealth,
} from "~/store/environments";

import {
  Connectable,
  ReconnectionState,
  WebsocketConnectionManager,
} from "./WebsocketConnectionManager";

function createHealthyEnvironment(httpAddress: string) {
  return {
    state: "enabled" as const,
    status: { health: "healthy" as const, version: "0.0.0", errors: [] },
    httpAddress,
    sqlAddress: "localhost:6876",
    resolvable: true,
    enabledAt: new Date().toISOString(),
    errors: [],
  };
}

function createMockConnectable(): Connectable & {
  closeCallbacks: Set<() => void>;
  openCallbacks: Set<() => void>;
  simulateClose: () => void;
  simulateOpen: () => void;
} {
  const closeCallbacks = new Set<() => void>();
  const openCallbacks = new Set<() => void>();
  let connected = false;

  return {
    closeCallbacks,
    openCallbacks,
    reconnect: vi.fn(() => {
      connected = true;
    }),
    disconnect: vi.fn(() => {
      connected = false;
    }),
    isConnected: vi.fn(() => connected),
    registerOnClose: vi.fn((cb) => {
      closeCallbacks.add(cb);
      return () => closeCallbacks.delete(cb);
    }),
    registerOnOpen: vi.fn((cb) => {
      openCallbacks.add(cb);
      return () => openCallbacks.delete(cb);
    }),
    simulateClose: () => {
      connected = false;
      closeCallbacks.forEach((cb) => cb());
    },
    simulateOpen: () => {
      connected = true;
      openCallbacks.forEach((cb) => cb());
    },
  };
}

describe("WebsocketConnectionManager", () => {
  let reconnectionStateAtom: ReturnType<typeof atom<ReconnectionState>>;
  let mockTarget: ReturnType<typeof createMockConnectable>;
  let manager: WebsocketConnectionManager | null;

  beforeEach(() => {
    vi.useFakeTimers();
    reconnectionStateAtom = atom<ReconnectionState>({
      status: "disconnected",
      attempt: 0,
      maxAttempts: 5,
      nextRetryMs: null,
    });
    mockTarget = createMockConnectable();
    manager = null;
    // Reset environment state
    getStore().set(environmentsWithHealth, undefined);
  });

  afterEach(() => {
    manager?.destroy();
    vi.useRealTimers();
  });

  describe("socket event handling", () => {
    it("resets retry count when socket opens", () => {
      const store = getStore();
      manager = new WebsocketConnectionManager(
        mockTarget,
        store,
        reconnectionStateAtom,
      );

      // Simulate successful open
      mockTarget.simulateOpen();

      const state = store.get(reconnectionStateAtom);
      expect(state.status).toBe("connected");
      expect(state.attempt).toBe(0);
    });

    it("schedules retry when socket closes and target is healthy", () => {
      const store = getStore();

      // Set up healthy environment
      store.set(
        environmentsWithHealth,
        new Map([
          ["aws/us-east-1", createHealthyEnvironment("localhost:6875")],
        ]) as unknown as EnvironmentsWithHealth,
      );

      manager = new WebsocketConnectionManager(
        mockTarget,
        store,
        reconnectionStateAtom,
        { baseDelayMs: 1000, maxAttempts: 3 },
      );

      mockTarget.simulateOpen();
      vi.mocked(mockTarget.reconnect).mockClear();

      // Close triggers retry scheduling
      mockTarget.simulateClose();

      const state = store.get(reconnectionStateAtom);
      expect(state.status).toBe("reconnecting");

      // Advance past retry delay
      vi.advanceTimersByTime(1500);
      expect(mockTarget.reconnect).toHaveBeenCalledTimes(1);
    });

    it("does not schedule retry when socket closes and environment is unhealthy", () => {
      const store = getStore();

      // No environment set up = unhealthy
      manager = new WebsocketConnectionManager(
        mockTarget,
        store,
        reconnectionStateAtom,
        { baseDelayMs: 1000, maxAttempts: 3 },
      );

      // Simulate a close without healthy environment
      mockTarget.simulateClose();

      const state = store.get(reconnectionStateAtom);
      expect(state.status).toBe("disconnected");

      // Advance time - no retry should be scheduled
      vi.advanceTimersByTime(5000);
      expect(mockTarget.reconnect).not.toHaveBeenCalled();
    });
  });

  describe("destroy", () => {
    it("disconnects and cleans up subscriptions", () => {
      const store = getStore();
      manager = new WebsocketConnectionManager(
        mockTarget,
        store,
        reconnectionStateAtom,
      );

      manager.destroy();
      manager = null;
      expect(mockTarget.disconnect).toHaveBeenCalled();
      expect(mockTarget.closeCallbacks.size).toBe(0);
      expect(mockTarget.openCallbacks.size).toBe(0);
    });
  });
});
