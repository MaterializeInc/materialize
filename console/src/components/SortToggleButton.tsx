// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Button, Text, useTheme } from "@chakra-ui/react";
import React from "react";

import { MaterializeTheme } from "~/theme";
import { SortOrder } from "~/utils/sort";

export interface SortToggleButtonProps {
  label: string;
  column: string;
  selectedSortColumn: string;
  selectedSortOrder: SortOrder;
  toggleSort: (column: string) => void;
}

const SortOrderIcon = (props: { order: SortOrder }) => {
  const { colors } = useTheme<MaterializeTheme>();

  return (
    <Text color={colors.accent.brightPurple}>
      {props.order === "asc" ? "↑" : "↓"}
    </Text>
  );
};

export const SortToggleButton = (props: SortToggleButtonProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const active = props.selectedSortColumn === props.column;

  return (
    <Button
      _active={{
        color: colors.accent.brightPurple,
      }}
      color={colors.accent.brightPurple}
      onClick={() => {
        props.toggleSort(props.column);
      }}
      size="sm"
      variant="link"
    >
      {props.label}
      {active && <SortOrderIcon order={props.selectedSortOrder} />}
    </Button>
  );
};
