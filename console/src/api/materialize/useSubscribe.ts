// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { PrimitiveAtom, useSetAtom } from "jotai";
import { RawBuilder } from "kysely";
import React from "react";

import { useAutomaticallyConnectSocket } from "~/hooks/useAutomaticallyConnectSocket";
import { getStore } from "~/jotai";
import { useCurrentEnvironmentHttpAddress } from "~/store/environments";

import { queryBuilder } from "./db";
import {
  SelectFunction,
  SubscribeError,
  SubscribeManager,
  SubscribeRow,
  SubscribeState,
  UpsertKeyFunction,
} from "./SubscribeManager";

export { buildSubscribeQuery } from "~/api/materialize/buildSubscribeQuery";

export type HandleRowCallback<T, R> = (result: SubscribeRow<T>) => R;

export type UseSubscribeOptions<T extends object, R> = {
  /** The SUBSCRIBE query built by {@link buildSubscribeQuery}. Pass `undefined` to skip connecting. */
  subscribe: RawBuilder<T> | undefined;
  /** If true, disconnects after the query completes. Useful for bounded subscribes. */
  closeSocketOnComplete?: boolean;
  /** Materialize cluster to run the subscribe on. Defaults to session cluster. */
  clusterName?: string;
  /** Transforms each row before storing. Commonly `(row) => row.data` to strip metadata. */
  select?: SelectFunction<T, R>;
};

export type UseSubscribeReturn<T> = {
  /** Closes the WebSocket but keeps the last snapshot in state. */
  disconnect: () => void;
  /** Clears all data and error state. Does not disconnect. */
  reset: () => void;
  /** The current materialized rows at the latest closed timestamp. */
  data: T[];
  /** True if an error has occurred (connection drop or server error). */
  isError: boolean;
  /** True once the initial snapshot is fully received. Before this, `data` may be incomplete. */
  snapshotComplete: boolean;
  /** The error details, if any. */
  error: SubscribeError | undefined;
};

/**
 * Opens a per-component SUBSCRIBE that lives for the component's lifetime.
 *
 * Creates a {@link SubscribeManager} on first render, connects via WebSocket,
 * and maintains a live materialized view of the query results in local state.
 * The connection is managed by {@link WebsocketConnectionManager} which
 * handles automatic reconnection with exponential backoff on network failures.
 *
 * **When to use this vs `useGlobalUpsertSubscribe`:**
 * - Use `useSubscribe` for data scoped to a single component or page
 *   (e.g., source statistics in the monitor page).
 * - Use `useGlobalUpsertSubscribe` for data shared across the entire app
 *   (e.g., catalog columns, indexes). Global subscribes are started once in
 *   `AppInitializer` and their data is accessed via Jotai atoms from any component.
 *
 * **Requirements:** The subscribe SQL must include `WITH (PROGRESS)` and
 * `ENVELOPE UPSERT`. Use {@link buildSubscribeQuery} to construct it.
 *
 * **Stability:** The `select` and `upsertKey` functions do not need to be
 * memoized. New function instances on re-render update the internal reference
 * without restarting the subscribe.
 *
 * @example
 * ```tsx
 * const { data, snapshotComplete } = useSubscribe({
 *   subscribe: buildSubscribeQuery(myQuery, { upsertKey: "id" }),
 *   select: (row) => row.data,
 *   upsertKey: (row) => row.data.id,
 * });
 *
 * if (!snapshotComplete) return <Spinner />;
 * return <List items={data} />;
 * ```
 */
export function useSubscribe<T extends object, R = SubscribeRow<T>>(
  options: UseSubscribeOptions<T, R> & {
    upsertKey: UpsertKeyFunction<T>;
  },
): UseSubscribeReturn<R> {
  const httpAddress = useCurrentEnvironmentHttpAddress();
  const request = useSubscribeRequest(options.subscribe);
  const [subscribe] = React.useState(
    new SubscribeManager<T, R>({
      request,
      httpAddress,
      upsert: {
        key: options.upsertKey,
      },
      select: options.select,
      sessionVariables: {
        cluster: options.clusterName,
      },
      closeSocketOnComplete: options.closeSocketOnComplete,
    }),
  );
  useAutomaticallyConnectSocket<T, R>({
    target: subscribe,
    subscribe,
    request,
  });

  const { data, error, snapshotComplete } = React.useSyncExternalStore(
    subscribe.onChange,
    subscribe.getSnapshot,
  );

  return {
    data,
    disconnect: subscribe.disconnect,
    reset: subscribe.reset,
    error,
    isError: Boolean(error),
    snapshotComplete,
  };
}

/**
 * Opens an app-wide SUBSCRIBE whose state lives in a Jotai atom, shared
 * across all components that read from that atom.
 *
 * **This is the foundation of the global catalog data layer.** Each call
 * creates one {@link SubscribeManager} and one WebSocket connection. The
 * results are written to the provided `atom`, which any component can read
 * via `useAtomValue(atom)`. The subscribe stays open for the app's lifetime
 * (or until the component calling this hook unmounts).
 *
 * Typically called once per data type in `AppInitializer.tsx`:
 *
 * @example
 * ```tsx
 * // In the store file (e.g., store/catalogColumns.ts):
 * export const catalogColumns = atom<SubscribeState<CatalogColumn>>({
 *   data: [], error: undefined, snapshotComplete: false,
 * });
 *
 * export function useSubscribeToCatalogColumns() {
 *   const subscribe = React.useMemo(() =>
 *     buildSubscribeQuery(buildAllColumnsQuery(), { upsertKey: ["objectId", "name"] }),
 *   []);
 *   return useGlobalUpsertSubscribe({
 *     atom: catalogColumns,
 *     subscribe,
 *     select: (row) => row.data,
 *     upsertKey: (row) => `${row.data.objectId}:${row.data.name}`,
 *   });
 * }
 *
 * // In AppInitializer.tsx:
 * useSubscribeToCatalogColumns();
 *
 * // In any component:
 * const { data, snapshotComplete } = useAtomValue(catalogColumns);
 * ```
 *
 * **Requirements:** Same as `useSubscribe` — the SQL must include
 * `WITH (PROGRESS)` and `ENVELOPE UPSERT`.
 *
 * @see useSubscribe — for per-component subscribes with local state
 */
export function useGlobalUpsertSubscribe<T extends object, R = SubscribeRow<T>>(
  options: UseSubscribeOptions<T, R> & {
    upsertKey: UpsertKeyFunction<T>;
    atom: PrimitiveAtom<SubscribeState<R>>;
  },
) {
  const setValue = useSetAtom(options.atom);
  const httpAddress = useCurrentEnvironmentHttpAddress();
  const request = useSubscribeRequest(options.subscribe);
  const [subscribe] = React.useState(
    new SubscribeManager<T, R>({
      request,
      httpAddress,
      upsert: {
        key: options.upsertKey,
      },
      sessionVariables: {
        cluster: options?.clusterName,
      },
      closeSocketOnComplete: options?.closeSocketOnComplete,
      select: options.select,
    }),
  );
  useAutomaticallyConnectSocket<T, R>({
    target: subscribe,
    subscribe,
    request,
  });

  React.useEffect(() => {
    const cleanup = subscribe.onChange(() => {
      const snapshot = subscribe.getSnapshot();
      if (getStore().get(options.atom) === snapshot) return;

      setValue(snapshot);
    });
    return cleanup;
  }, [options.atom, setValue, subscribe]);

  return {
    subscribe,
  };
}

/**
 * Like {@link useSubscribe} but without upsert deduplication — raw updates
 * are appended for each closed timestamp. Used for streaming data where
 * you need the full append log (e.g., source/sink statistics time series).
 *
 * Most callers should prefer `useSubscribe` (with upsert) for catalog data,
 * or `useGlobalUpsertSubscribe` for app-wide shared data.
 */
export function useSubscribeManager<T extends object, R = SubscribeRow<T>>({
  subscribe,
  ...options
}: UseSubscribeOptions<T, R>): UseSubscribeReturn<R> {
  const httpAddress = useCurrentEnvironmentHttpAddress();
  const request = useSubscribeRequest(subscribe);
  const [subscribeInstance] = React.useState(
    new SubscribeManager<T, R>({
      request,
      httpAddress,
      sessionVariables: {
        cluster: options?.clusterName,
      },
      closeSocketOnComplete: options?.closeSocketOnComplete,
      select: options?.select,
    }),
  );
  useAutomaticallyConnectSocket<T, R>({
    target: subscribeInstance,
    subscribe: subscribeInstance,
    request,
    getSessionVariables: () => ({
      cluster: options?.clusterName,
    }),
  });

  const { data, error, snapshotComplete } = React.useSyncExternalStore(
    subscribeInstance.onChange,
    subscribeInstance.getSnapshot,
  );

  return {
    data,
    disconnect: subscribeInstance.disconnect,
    reset: subscribeInstance.reset,
    error,
    isError: Boolean(error),
    snapshotComplete,
  };
}

function useSubscribeRequest(subscribe: RawBuilder<unknown> | undefined) {
  return React.useMemo(() => {
    if (!subscribe) return undefined;

    const compiled = subscribe.compile(queryBuilder);
    return {
      queries: [
        {
          query: compiled.sql,
          params: compiled.parameters as string[],
        },
      ],
    };
  }, [subscribe]);
}
