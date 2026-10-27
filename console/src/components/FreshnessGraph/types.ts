// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { IPostgresInterval } from "~/api/materialize";

type LagInfoBase = {
  // Whether the lag is queryable.
  // This is false when the lag is null, which happens when an index isn't hydrated or
  // when there is no readable time for a table / mv / source.
  // In either case, it's not queryable.
  queryable: boolean;
  schemaName: string | null;
  objectName: string | null;
};

export type LagInfo = LagInfoBase & {
  queryable: true;
  totalMs: number;
  interval: IPostgresInterval;
};

export type NonQueryableLagInfo = LagInfoBase & {
  queryable: false;
};

export type DataPoint = {
  timestamp: number;
  lag: Record<string, LagInfo | NonQueryableLagInfo>;
};

export interface GraphLineSeries {
  key: string;
  label: string;
  yAccessor: (d: DataPoint) => number | null;
}
