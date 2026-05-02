// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SinkStatisticsRow } from "../queries";

export function calculateRate(
  current: SinkStatisticsRow | undefined,
  previous: SinkStatisticsRow | undefined,
  key: keyof Omit<SinkStatisticsRow["data"], "id">,
) {
  if (current === undefined || previous === undefined) return null;

  const timeDiff = current.mzTimestamp - previous.mzTimestamp;
  const currentDatum = Number(current.data[key]);
  const previousDataum = Number(previous.data[key]);
  if (currentDatum === null || previousDataum === null) return null;

  const result = ((currentDatum - previousDataum) / timeDiff) * 1000;
  if (Number.isNaN(result)) return 0;
  if (result < 0) return 0;
  return result;
}
