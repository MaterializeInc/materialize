// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * @module
 * Factory for creating global catalog subscribe stores.
 *
 * Each store maintains a live replica of a Materialize system catalog table
 * via a long-lived WebSocket SUBSCRIBE, with the results stored in a Jotai
 * atom accessible from any component.
 */

import { atom, type PrimitiveAtom } from "jotai";
import type { Compilable, RawBuilder } from "kysely";
import React from "react";

import type {
  SubscribeRow,
  SubscribeState,
} from "~/api/materialize/SubscribeManager";
import {
  buildSubscribeQuery,
  useGlobalUpsertSubscribe,
} from "~/api/materialize/useSubscribe";

interface CatalogStoreOptions<T extends object> {
  /** Returns the SQL query to subscribe to. Called once inside `useMemo`. */
  query: () => Compilable<T> | RawBuilder<unknown>;
  /** Column name(s) forming the upsert key. */
  upsertKey: (keyof T & string) | (keyof T & string)[];
}

interface CatalogStore<T extends object> {
  /** Jotai atom holding the live subscribe state. Read with `useAtomValue`. */
  atom: PrimitiveAtom<SubscribeState<T>>;
  /** Hook to start the SUBSCRIBE. Call once in AppInitializer. */
  useSubscribe: () => void;
}

/**
 * Creates a global catalog subscribe store: a Jotai atom paired with an
 * initializer hook that populates it via a long-lived WebSocket SUBSCRIBE.
 *
 * The upsert key function is derived from `upsertKey` so callers specify
 * the key column names once.
 */
export function createCatalogStore<T extends object>(
  options: CatalogStoreOptions<T>,
): CatalogStore<T> {
  const storeAtom = atom<SubscribeState<T>>({
    data: [],
    error: undefined,
    snapshotComplete: false,
  });

  const upsertKeyFn = buildUpsertKeyFunction<T>(options.upsertKey);

  function useSubscribe() {
    const subscribe = React.useMemo(
      () =>
        buildSubscribeQuery(options.query(), {
          upsertKey: options.upsertKey,
        }),
      [],
    );

    useGlobalUpsertSubscribe({
      atom: storeAtom,
      subscribe,
      select: (row: SubscribeRow<T>) => row.data,
      upsertKey: upsertKeyFn,
    });
  }

  return { atom: storeAtom, useSubscribe };
}

function buildUpsertKeyFunction<T extends object>(
  keyConfig: (keyof T & string) | (keyof T & string)[],
): (row: SubscribeRow<T>) => string {
  if (typeof keyConfig === "string") {
    return (row) => String(row.data[keyConfig]);
  }
  return (row) => keyConfig.map((k) => String(row.data[k])).join(":");
}
