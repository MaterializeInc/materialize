// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ProvidedContext } from "vitest";

import { getMaterializeEndpoints } from "./mzcompose";

export async function setup({
  provide,
}: {
  provide: <T extends keyof ProvidedContext>(
    key: T,
    value: ProvidedContext[T],
  ) => void;
}) {
  // Cache these values to be used in the tests
  const value = await getMaterializeEndpoints();
  provide("materializeEndpoints", value);
}
