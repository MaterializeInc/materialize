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
  subscribe: RawBuilder<T> | undefined;
  closeSocketOnComplete?: boolean;
  clusterName?: string;
  select?: SelectFunction<T, R>;
};

export type UseSubscribeReturn<T> = {
  /** disconnects the socket but keeps state */
  disconnect: () => void;
  /** Clears all state but does not disconnect */
  reset: () => void;
  data: T[];
  isError: boolean;
  snapshotComplete: boolean;
  error: SubscribeError | undefined;
};

/**
 * Executes a subscribe query and handles state internally. All updates are reduced to
 * the current set of values at latest closed timestamp.
 *
 * Note that the subscribe statement must have WITH (PROGRESS) and ENVELOPE UPSERT.
 *
 * The `select` and `upsertKey` functions are not expected to be stable, and new
 * function instances for these options will not restart the subscribe. On each render,
 * the socket will update the function reference.
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
 * Executes a subscribe query, storing state in the provided atom.
 * All updates are reduced to the current set of values at latest closed timestamp.
 *
 * Note that the subscribe statement must have WITH (PROGRESS) and ENVELOPE UPSERT.
 *
 * The `select` and `upsertKey` functions are not expected to be stable, and new
 * function instances for these options will not restart the subscribe. On each render,
 * the socket will update the function reference.
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
 * Executes a subscribe query and handles state internally. The raw updates are flushed
 * for each closed timestamp.
 *
 * Note that the subscribe statement must have WITH (PROGRESS) and ENVELOPE UPSERT.
 *
 * The `select` and `upsertKey` functions are not expected to be stable, and new
 * function instances for these options will not restart the subscribe. On each render,
 * the socket will update the function reference.
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
    sessionVariables: {
      cluster: options?.clusterName,
    },
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
