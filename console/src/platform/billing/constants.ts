// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { DailyCostKey } from "~/api/cloudGlobalApi";

export const INVOICE_FETCH_ERROR_MESSAGE =
  "An error occurred loading invoices.";

export const FORBIDDEN_PAYMENT_ERROR_MESSAGE =
  "Only organization admins can manage payment methods. Contact an admin to make changes.";

export const ACCOUNT_SPEND_FETCH_ERROR_MESSAGE =
  "An error occurred loading your usage";

export const ROLLING_AVG_TIME_RANGE_LOOKBACK_DAYS = 30;

export const costUnits = {
  compute: "credits",
  storage: "GB",
} as { [key in DailyCostKey]: string };

// Left padding for an indented child row in a spend table: the cell's own
// padding, plus the caret width and the gap between caret and label, so child
// labels line up just past the parent row's caret.
export const resourceTypePaddingLeft = 4 + 4 + 2;

// Shared cell layout for the spend tables (SpendBreakdown / AccountCluster
// breakdown). Callers set `borderColor` for the contexts where the divider
// should show.
export const baseCellStyles = {
  px: 4,
  my: "auto",
  display: "flex",
  alignItems: "center",
  height: 8,
  borderBottom: "1px solid",
};

export const replicaSorts = new Map([
  // These are mapped to their corresponding centicredit value.
  // See: https://github.com/MaterializeInc/cloud/blob/main/doc/design/20231004_cluster_sizings.md
  ["3xsmall", 25],
  ["2xsmall", 50],
  ["xsmall", 100],
  ["small", 200],
  ["medium", 400],
  ["large", 800],
  ["xlarge", 1_600],
  ["2xlarge", 3_200],
  ["3xlarge", 6_400],
  ["4xlarge", 12_800],
  ["5xlarge", 25_600],
  ["6xlarge", 51_200],
]);
