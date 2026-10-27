// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { IPostgresInterval } from "~/api/materialize";
import { sumPostgresIntervalMs } from "~/util";

type LagGeneric = { lag: IPostgresInterval | null };

export function sortLagInfo<A extends LagGeneric, B extends LagGeneric>(
  a: A,
  b: B,
) {
  const aLag = a.lag !== null ? sumPostgresIntervalMs(a.lag) : 0;
  const bLag = b.lag !== null ? sumPostgresIntervalMs(b.lag) : 0;

  return bLag - aLag;
}
