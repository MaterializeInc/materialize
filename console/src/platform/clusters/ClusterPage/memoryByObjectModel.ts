// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { clamp } from "~/util";

/** How many of the largest objects the card shows. */
export const TOP_OBJECT_COUNT = 10;

/** One object on the card, from the polled or the streamed sizes. */
export interface LargestObject {
  id: string | null;
  name: string | null;
  /** Bytes: a bigint when polled, a number when streamed. */
  size: bigint | number | null;
  memoryPercentage: number | null;
  type: string | null;
  schemaName: string | null;
  databaseName: string | null;
  dataflowId: string | null;
  dataflowName: string | null;
  isOrphanedDataflow: boolean;
}

/**
 * Bar segment widths for objects' shares of the heap limit, in order. The bar
 * stops at the heap limit even if the shares add up to more.
 */
export const segmentWidths = (percentages: Array<number | null>) => {
  let remaining = 100;
  return percentages.map((percentage) => {
    const width = clamp(percentage ?? 0, 0, remaining);
    remaining -= width;
    return width;
  });
};
