// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Box,
  Grid,
  Spinner,
  Text,
  useDisclosure,
  useTheme,
} from "@chakra-ui/react";
import React from "react";

import {
  CostBreakdown,
  CostBreakdownAccount,
  CostBreakdownCluster,
} from "~/api/cloudGlobalApi";
import ErrorBox from "~/components/ErrorBox";
import ChevronRightIcon from "~/svg/ChevronRightIcon";
import { MaterializeTheme } from "~/theme";
import { formatCurrency } from "~/utils/format";

import { baseCellStyles, resourceTypePaddingLeft } from "./constants";
import { SafariSafeCollapse } from "./SpendBreakdown";

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

function accountTotal(account: CostBreakdownAccount): number {
  return account.clusters.reduce(
    (sum, cluster) => sum + clusterTotal(cluster.amounts),
    0,
  );
}

/**
 * Region-qualified label for a cluster row, e.g. "aws/us-east-1 / quickstart.r1",
 * "aws/us-east-1 / Storage", or "aws/us-east-1 / Egress" — mirroring the daily
 * "Spend between …" table's `{region} / {resourceType}` format. Compute clusters
 * carry a `cluster.replica` grouping key; storage/egress rows have an empty key
 * and instead carry a `category` ("Storage" / "Egress") so the two render as
 * distinct rows (see interface.rs). The final "Other" is a defensive fallback
 * for the unexpected case where neither is set.
 */
function clusterLabel(cluster: CostBreakdownCluster): string {
  const label = cluster.cluster_grouping_key || cluster.category || "Other";
  return `${cluster.region} / ${label}`;
}

/**
 * A single account rendered as a collapsible group, mirroring SpendBreakdown's
 * `ResourceGroup`: the account is the always-visible parent row (caret + total)
 * and its clusters are indented child rows revealed by the disclosure. Groups
 * default open so the breakdown is visible without interaction.
 */
const AccountGroup = ({ account }: { account: CostBreakdownAccount }) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { isOpen, onToggle } = useDisclosure({ defaultIsOpen: true });

  const groupHeaderStyles = {
    ...baseCellStyles,
    height: 16,
    textStyle: "heading-xs",
    borderBottom: 0,
    borderTop: "1px solid",
    borderColor: colors.border.secondary,
  };

  return (
    <>
      <Box
        display="contents"
        role="row"
        onClick={onToggle}
        cursor="pointer"
        data-testid="account-row"
      >
        <Box {...groupHeaderStyles} role="cell">
          <ChevronRightIcon
            width="4"
            height="4"
            transform={`rotate(${isOpen ? 90 : 0}deg)`}
            transition="all 0.1s"
            marginRight="2"
          />
          {account.external_customer_id}
        </Box>
        <Box {...groupHeaderStyles} role="cell" justifyContent="end">
          {formatCurrency(accountTotal(account))}
        </Box>
      </Box>
      <SafariSafeCollapse
        isCollapsed={!isOpen}
        rowCount={account.clusters.length}
      >
        {account.clusters.map((cluster, ix) => {
          const isLastElement = ix === account.clusters.length - 1;
          const cellStyles = {
            ...baseCellStyles,
            borderColor: "transparent",
            height: isLastElement ? 10 : baseCellStyles.height,
            paddingBottom: isLastElement ? "8px" : "unset",
          };
          return (
            <React.Fragment
              key={`${cluster.environment_id}/${cluster.cluster_grouping_key}/${ix}`}
            >
              <Box
                {...cellStyles}
                paddingLeft={resourceTypePaddingLeft}
                whiteSpace="nowrap"
                role="cell"
              >
                {clusterLabel(cluster)}
              </Box>
              <Box {...cellStyles} role="cell" justifyContent="end">
                {formatCurrency(clusterTotal(cluster.amounts))}
              </Box>
            </React.Fragment>
          );
        })}
      </SafariSafeCollapse>
    </>
  );
};

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

  const headerStyles = {
    ...baseCellStyles,
    height: 10,
    textStyle: "text-ui-med",
    color: colors.foreground.secondary,
    borderColor: colors.border.secondary,
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
          borderBottom="1px solid"
          borderBottomColor={colors.border.secondary}
        >
          <Box {...headerStyles} role="columnheader">
            Account / cluster
          </Box>
          <Box {...headerStyles} role="columnheader" justifyContent="end">
            Total cost
          </Box>
          {breakdown.accounts.map((account) => (
            <AccountGroup
              key={account.external_customer_id}
              account={account}
            />
          ))}
        </Grid>
      )}
    </Box>
  );
};

export default AccountClusterBreakdown;
