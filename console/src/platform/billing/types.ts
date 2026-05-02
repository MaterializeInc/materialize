// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { DailyCostKey } from "~/api/cloudGlobalApi";

export type RegionGroupedSummary = {
  total: number;
  regions: Map<string, number>;
};

type ResourceItem = {
  usageValue: number;
  usageUnits: string;
  usagePoints: number[];
  totalCost: number;
};

export type ResourceMeasurement = ResourceItem & {
  sort: number;
  rate: string;
};

export type ResourceBreakdown = {
  [k in DailyCostKey]: {
    total: ResourceItem;
    resources: Map<string, ResourceMeasurement>;
  };
};

export type RegionResourceBreakdown = Map<string, ResourceBreakdown>;
