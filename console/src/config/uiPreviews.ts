// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * UI previews users can opt into from the profile menu. Each entry pairs the
 * LaunchDarkly flag that offers the preview with the label shown on its
 * toggle. Opting in is per browser, see useUiPreview.
 *
 * To put a UI behind a preview toggle, add an entry here and render the new
 * UI when `useUiPreview(<key>).isEnabled`.
 */
export const UI_PREVIEWS = {
  clusterListUsageMetrics: {
    ldFlag: "usage-metrics-in-cluster-list-CNS121",
    label: "Cluster list usage metrics",
  },
} as const satisfies Record<string, { ldFlag: string; label: string }>;

export type UiPreviewKey = keyof typeof UI_PREVIEWS;
