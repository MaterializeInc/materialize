// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// because the data is sampled on 60s intervals, we don't want to show more granular data than this.
export const MIN_BUCKET_SIZE_MS = 60 * 1000;

export const CLUSTERS_FETCH_ERROR_MESSAGE =
  "An error occurred loading cluster data";

export const CLUSTER_METRICS_UNAVAILABLE_MESSAGE = `Cluster metrics are unavailable.
  This may be due to a temporary issue with the cluster or because pod metrics collection is disabled.
  If you are the administrator of this cluster, please ensure that pod metrics collection is enabled and that metrics-server is installed.`;
