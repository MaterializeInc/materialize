// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createStore } from "jotai";

let store = createStore();

/**
 * Returns the current Jotai store. Note that it's not safe to subscribe directly to the
 * store, since we create a new store instance when you switch organization.
 */
export function getStore() {
  return store;
}

/**
 * Creates a new Jotai store in module scope, resetting all global state.
 * This function won't actually change the value used by the provider, you probably want
 * ResetStoreContext instead.
 */
export function resetStore() {
  store = createStore();
}
