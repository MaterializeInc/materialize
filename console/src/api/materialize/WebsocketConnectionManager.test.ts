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
  currentRegionIdAtom,
  EnvironmentsWithHealth,
  environmentsWithHealth,
} from "~/store/environments";
import { defaultRegionId } from "~/test/utils";

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

    it("schedules another retry when reconnect throws", () => {
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

      // Make reconnect throw on next call (e.g. network blocked)
      vi.mocked(mockTarget.reconnect).mockImplementationOnce(() => {
        throw new Error("Failed to construct WebSocket");
      });

      // Close triggers retry scheduling
      mockTarget.simulateClose();

      // Advance past first retry delay — reconnect throws but doesn't break the chain
      vi.advanceTimersByTime(1500);
      expect(mockTarget.reconnect).toHaveBeenCalledTimes(1);

      // Advance past second retry — reconnect succeeds this time
      vi.advanceTimersByTime(5000);
      expect(mockTarget.reconnect).toHaveBeenCalledTimes(2);
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

    it("does not start a second connect while a handshake is in progress", () => {
      const store = getStore();
      store.set(
        environmentsWithHealth,
        new Map([
          ["aws/us-east-1", createHealthyEnvironment("localhost:6875")],
        ]) as unknown as EnvironmentsWithHealth,
      );

      // Constructor starts a connect; handshake is still in progress.
      manager = new WebsocketConnectionManager(
        mockTarget,
        store,
        reconnectionStateAtom,
      );
      expect(mockTarget.reconnect).toHaveBeenCalledTimes(1);

      // Overlapping reconnect is dropped while the handshake is in progress.
      manager.reconnect();
      expect(mockTarget.reconnect).toHaveBeenCalledTimes(1);

      // Once the handshake completes, the next reconnect proceeds.
      mockTarget.simulateOpen();
      manager.reconnect();
      expect(mockTarget.reconnect).toHaveBeenCalledTimes(2);
    });
  });

  describe("region switch", () => {
    const twoRegions = () =>
      new Map([
        [defaultRegionId, createHealthyEnvironment("addr-a:6876")],
        ["aws/eu-west-1", createHealthyEnvironment("addr-b:6876")],
      ]) as unknown as EnvironmentsWithHealth;

    afterEach(() => {
      getStore().set(currentRegionIdAtom, defaultRegionId);
    });

    it("reconnects at the new address when the region switches while connected", () => {
      const store = getStore();
      store.set(environmentsWithHealth, twoRegions());

      manager = new WebsocketConnectionManager(
        mockTarget,
        store,
        reconnectionStateAtom,
      );
      mockTarget.simulateOpen();
      vi.mocked(mockTarget.reconnect).mockClear();

      store.set(currentRegionIdAtom, "aws/eu-west-1");

      expect(mockTarget.reconnect).toHaveBeenCalledTimes(1);
      expect(mockTarget.reconnect).toHaveBeenCalledWith(
        "addr-b:6876",
        undefined,
      );
    });

    it("reconnects after a round trip through an unhealthy region", () => {
      const store = getStore();
      const crashed = {
        ...createHealthyEnvironment("addr-b:6876"),
        status: { health: "crashed" as const, version: "0.0.0", errors: [] },
      };
      store.set(
        environmentsWithHealth,
        new Map<string, unknown>([
          [defaultRegionId, createHealthyEnvironment("addr-a:6876")],
          ["aws/eu-west-1", crashed],
        ]) as unknown as EnvironmentsWithHealth,
      );

      manager = new WebsocketConnectionManager(
        mockTarget,
        store,
        reconnectionStateAtom,
      );
      mockTarget.simulateOpen();
      vi.mocked(mockTarget.reconnect).mockClear();

      // Pausing on the unhealthy region tears the old region's socket down.
      store.set(currentRegionIdAtom, "aws/eu-west-1");
      expect(mockTarget.disconnect).toHaveBeenCalled();
      expect(mockTarget.reconnect).not.toHaveBeenCalled();

      // Returning to the healthy region reconnects rather than assuming the
      // old socket still serves it.
      store.set(currentRegionIdAtom, defaultRegionId);
      expect(mockTarget.reconnect).toHaveBeenCalledWith(
        "addr-a:6876",
        undefined,
      );
    });

    it("keeps the socket through a same-region health blip", () => {
      const store = getStore();
      store.set(environmentsWithHealth, twoRegions());
      manager = new WebsocketConnectionManager(
        mockTarget,
        store,
        reconnectionStateAtom,
      );
      mockTarget.simulateOpen();
      vi.mocked(mockTarget.reconnect).mockClear();
      vi.mocked(mockTarget.disconnect).mockClear();

      // The current region's health check fails: the socket is left alone,
      // so consumers like the Shell see no silent teardown.
      store.set(
        environmentsWithHealth,
        new Map<string, unknown>([
          [
            defaultRegionId,
            {
              ...createHealthyEnvironment("addr-a:6876"),
              status: {
                health: "crashed" as const,
                version: "0.0.0",
                errors: [],
              },
            },
          ],
          ["aws/eu-west-1", createHealthyEnvironment("addr-b:6876")],
        ]) as unknown as EnvironmentsWithHealth,
      );
      expect(mockTarget.disconnect).not.toHaveBeenCalled();

      // Health recovers: the socket is still connected, so no churn either.
      store.set(environmentsWithHealth, twoRegions());
      expect(mockTarget.reconnect).not.toHaveBeenCalled();
    });

    it("reconnects after a round trip through a disabled region", () => {
      const store = getStore();
      store.set(
        environmentsWithHealth,
        new Map<string, unknown>([
          [defaultRegionId, createHealthyEnvironment("addr-a:6876")],
          ["aws/eu-west-1", { state: "disabled" as const }],
        ]) as unknown as EnvironmentsWithHealth,
      );
      manager = new WebsocketConnectionManager(
        mockTarget,
        store,
        reconnectionStateAtom,
      );
      mockTarget.simulateOpen();
      vi.mocked(mockTarget.reconnect).mockClear();

      // A disabled region never updates the address, so the teardown must key
      // on the region, not the address.
      store.set(currentRegionIdAtom, "aws/eu-west-1");
      expect(mockTarget.disconnect).toHaveBeenCalled();

      store.set(currentRegionIdAtom, defaultRegionId);
      expect(mockTarget.reconnect).toHaveBeenCalledWith(
        "addr-a:6876",
        undefined,
      );
    });

    it("reconnects after a region switch that lands mid-handshake", () => {
      const store = getStore();
      store.set(environmentsWithHealth, twoRegions());

      // Constructor starts a connect to region A; handshake still in flight.
      manager = new WebsocketConnectionManager(
        mockTarget,
        store,
        reconnectionStateAtom,
      );
      expect(mockTarget.reconnect).toHaveBeenCalledWith(
        "addr-a:6876",
        undefined,
      );
      vi.mocked(mockTarget.reconnect).mockClear();

      // Switch regions before the handshake completes: deferred, not dropped.
      store.set(currentRegionIdAtom, "aws/eu-west-1");
      expect(mockTarget.reconnect).not.toHaveBeenCalled();

      mockTarget.simulateOpen();
      expect(mockTarget.reconnect).toHaveBeenCalledWith(
        "addr-b:6876",
        undefined,
      );
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
