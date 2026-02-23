// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export type GraphDataKey =
  | "memoryPercent"
  | "cpuPercent"
  | "diskPercent"
  | "maxMemoryAndDiskPercent"
  | "heapPercent";

export interface DataPoint {
  id: string;
  name: string;
  size: string | null;
  bucketStart: number;
  bucketEnd: number;
  cpuPercent: number | null;
  memoryPercent: number | null;
  heapPercent: number | null;
  diskPercent: number | null;
  maxMemoryAndDiskPercent: number | null;
  offlineEvents: OfflineEvent[];
}

export interface OfflineEvent {
  id: string;
  offlineReason: string | null;
  status: string;
  timestamp: number;
}

export interface ReplicaData {
  id: string;
  data: DataPoint[];
}
