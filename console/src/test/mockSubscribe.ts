// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SubscribeState } from "~/api/materialize/SubscribeManager";
import { UseSubscribeReturn } from "~/api/materialize/useSubscribe";

export type UpsertSubscribeReturn<T> = Omit<UseSubscribeReturn<T>, "data"> & {
  data: T[];
};

export function mockUpsertSubscribe<T>(
  overrides?: Partial<UpsertSubscribeReturn<T>>,
): UpsertSubscribeReturn<T> {
  return {
    disconnect: vi.fn(),
    reset: vi.fn(),
    data: [],
    isError: false,
    snapshotComplete: true,
    error: undefined,
    ...overrides,
  };
}

export function mockSubscribeState<T>(
  overrides?: Partial<SubscribeState<T>>,
): SubscribeState<T> {
  return {
    data: [],
    snapshotComplete: true,
    error: undefined,
    ...overrides,
  };
}
