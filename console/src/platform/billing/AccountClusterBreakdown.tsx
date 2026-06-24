// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Grid, Spinner, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { CostBreakdown } from "~/api/cloudGlobalApi";
import ErrorBox from "~/components/ErrorBox";
import { MaterializeTheme } from "~/theme";
import { formatCurrency } from "~/utils/format";

type AccountClusterBreakdownProps = {
  breakdown: CostBreakdown | null;
  isLoading: boolean;
  isError: boolean;
  error: Error | null;
};

/** Sum a cluster's per-price amounts (dollar strings) into a single total. */
function clusterTotal(amounts: { [priceId: string]: string }): number {
  return Object.values(amounts).reduce(
    (sum, amount) => sum + parseFloat(amount),
    0,
  );
}

/**
 * Per-account, per-cluster cost breakdown (Phase 1 / SAS-128). Augments the
 * existing daily chart: a parent org sees itself plus each child account, a
 * child sees only its own account, and a standalone org sees a single account.
 * Shows one total per cluster (the sum of its price amounts) over the selected
 * period — a compute/storage split is deferred to a later phase.
 */
const AccountClusterBreakdown = ({
  breakdown,
  isLoading,
  isError,
  error,
}: AccountClusterBreakdownProps) => {
  const { colors } = useTheme<MaterializeTheme>();

  const baseCellStyles = {
    px: 4,
    display: "flex",
    alignItems: "center",
    height: 10,
    borderBottom: "1px solid",
    borderColor: colors.border.secondary,
  };
  const headerStyles = {
    ...baseCellStyles,
    textStyle: "text-ui-med",
    color: colors.foreground.secondary,
  };

  return (
    <Box data-testid="account-cluster-breakdown">
      <Text textStyle="heading-sm" mb={4}>
        Spend by account &amp; cluster
      </Text>
      {isLoading ? (
        <Spinner data-testid="account-breakdown-loading" />
      ) : isError ? (
        <ErrorBox
          message={error?.message || "There was an error fetching your usage."}
        />
      ) : !breakdown || breakdown.accounts.length === 0 ? (
        <Text
          textStyle="text-ui-reg"
          color={colors.foreground.secondary}
          data-testid="account-breakdown-empty"
        >
          No usage to break down for the selected period.
        </Text>
      ) : (
        <Grid
          gridTemplateColumns="minmax(250px, 1fr) minmax(120px, auto)"
          role="table"
          borderTop="1px solid"
          borderTopColor={colors.border.secondary}
        >
          <Box {...headerStyles} role="columnheader">
            Account / cluster
          </Box>
          <Box {...headerStyles} role="columnheader" justifyContent="end">
            Total cost
          </Box>
          {breakdown.accounts.map((account) => {
            const accountTotal = account.clusters.reduce(
              (sum, cluster) => sum + clusterTotal(cluster.amounts),
              0,
            );
            return (
              <React.Fragment key={account.external_customer_id}>
                <Box
                  {...baseCellStyles}
                  textStyle="heading-xs"
                  role="cell"
                  data-testid="account-row"
                >
                  {account.external_customer_id}
                </Box>
                <Box
                  {...baseCellStyles}
                  textStyle="heading-xs"
                  role="cell"
                  justifyContent="end"
                >
                  {formatCurrency(accountTotal)}
                </Box>
                {account.clusters.map((cluster, ix) => (
                  <React.Fragment
                    key={`${cluster.environment_id}/${cluster.cluster_grouping_key}/${ix}`}
                  >
                    <Box {...baseCellStyles} pl={10} role="cell">
                      {cluster.cluster_grouping_key || cluster.environment_id}
                    </Box>
                    <Box {...baseCellStyles} role="cell" justifyContent="end">
                      {formatCurrency(clusterTotal(cluster.amounts))}
                    </Box>
                  </React.Fragment>
                ))}
              </React.Fragment>
            );
          })}
        </Grid>
      )}
    </Box>
  );
};

export default AccountClusterBreakdown;
