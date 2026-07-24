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
  BoxProps,
  Collapse,
  Grid,
  GridProps,
  useDisclosure,
  useTheme,
} from "@chakra-ui/react";
import React, { PropsWithChildren, ReactNode } from "react";

import ChevronRightIcon from "~/svg/ChevronRightIcon";
import { MaterializeTheme } from "~/theme";
import { isSafari } from "~/util";

// Cell padding + caret width + caret/label gap, so an indented child label
// lines up just past its parent row's caret.
const INDENTED_PADDING_LEFT = 4 + 4 + 2;

const baseCellStyles = {
  px: 4,
  my: "auto",
  display: "flex",
  alignItems: "center",
  height: 8,
  borderBottom: "1px solid",
} as const;

export interface LedgerCellProps extends PropsWithChildren, BoxProps {
  variant?: "row" | "columnHeader" | "groupHeader" | "total";
  /** Right-align numeric content (costs, shares). */
  numeric?: boolean;
  /** Indent a child row's first cell past the parent's caret. */
  indented?: boolean;
  /** Last child row of a group: taller, with bottom padding. */
  isLastRow?: boolean;
}

export const LedgerCell = ({
  variant = "row",
  numeric,
  indented,
  isLastRow,
  children,
  ...rest
}: LedgerCellProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const variantStyles = {
    row: {
      borderColor: "transparent",
      ...(isLastRow && { height: 10, paddingBottom: "8px" }),
    },
    columnHeader: {
      height: 10,
      textStyle: "text-ui-med",
      color: colors.foreground.secondary,
      borderColor: colors.border.secondary,
    },
    groupHeader: {
      height: 16,
      textStyle: "heading-xs",
      borderBottom: 0,
      borderTop: "1px solid",
      borderColor: colors.border.secondary,
    },
    total: {
      height: 12,
      textStyle: "text-ui-med",
      borderBottom: 0,
      borderTop: "1px solid",
      borderColor: colors.border.secondary,
    },
  }[variant];
  return (
    <Box
      {...baseCellStyles}
      {...variantStyles}
      role={variant === "columnHeader" ? "columnheader" : "cell"}
      justifyContent={numeric ? "end" : undefined}
      paddingLeft={indented ? INDENTED_PADDING_LEFT : undefined}
      whiteSpace={indented ? "nowrap" : undefined}
      {...rest}
    >
      {children}
    </Box>
  );
};

export interface LedgerColumn {
  label: ReactNode;
  numeric?: boolean;
  /** Indent to align with an indented child row's label below it. */
  indented?: boolean;
}

export interface LedgerTableProps extends PropsWithChildren, GridProps {
  /** grid-template-columns; every row must render one cell per column. */
  templateColumns: string;
  columns: LedgerColumn[];
}

/**
 * CSS-grid table for grouped spend ledgers. A grid rather than Chakra Table
 * or UniversalTable because group collapse must keep child cells
 * column-aligned mid-animation, which needs subgrid rows — <tbody> can't
 * animate a run of <tr>s. See SafariSafeCollapse.
 */
export const LedgerTable = ({
  templateColumns,
  columns,
  children,
  ...gridProps
}: LedgerTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  return (
    <Grid
      role="table"
      gridTemplateColumns={templateColumns}
      borderBottom="1px solid"
      borderBottomColor={colors.border.secondary}
      {...gridProps}
    >
      <Box display="contents" role="row">
        {columns.map((column, ix) => (
          <LedgerCell
            key={ix}
            variant="columnHeader"
            numeric={column.numeric}
            indented={column.indented}
          >
            {column.label}
          </LedgerCell>
        ))}
      </Box>
      {children}
    </Grid>
  );
};

/** Disclosure caret for a group header's first cell. */
export const LedgerCaret = ({ isOpen }: { isOpen: boolean }) => (
  <ChevronRightIcon
    width="4"
    height="4"
    transform={`rotate(${isOpen ? 90 : 0}deg)`}
    transition="all 0.1s"
    marginRight="2"
  />
);

export interface LedgerGroupProps extends PropsWithChildren {
  /**
   * Cells of the always-visible parent row. Put
   * `<LedgerCaret isOpen={isOpen} />` at the start of the first cell.
   */
  renderHeader: (isOpen: boolean) => ReactNode;
  /** Child row count; bounds the collapse animation height. */
  rowCount: number;
  defaultIsOpen?: boolean;
  /** Keep the group collapsed while its data is still loading. */
  isLoading?: boolean;
  "data-testid"?: string;
  /** data-testid for the collapsible child-row region. */
  contentTestId?: string;
}

/** An expandable group: clickable parent row plus collapsible child rows. */
export const LedgerGroup = ({
  renderHeader,
  rowCount,
  defaultIsOpen = true,
  isLoading,
  "data-testid": testId,
  contentTestId,
  children,
}: LedgerGroupProps) => {
  const { isOpen, onToggle } = useDisclosure({ defaultIsOpen });
  return (
    <>
      <Box
        display="contents"
        role="row"
        aria-expanded={isOpen}
        tabIndex={0}
        onClick={onToggle}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            onToggle();
          }
        }}
        cursor="pointer"
        data-testid={testId}
      >
        {renderHeader(isOpen)}
      </Box>
      <SafariSafeCollapse
        isCollapsed={!isOpen || !!isLoading}
        rowCount={rowCount}
        data-testid={contentTestId}
      >
        {children}
      </SafariSafeCollapse>
    </>
  );
};

/**
 * Safari fails to layout subgrid elements inside a Collapse; fall back to
 * show/hide there.
 * https://stackoverflow.com/q/77927259/214197
 */
export const SafariSafeCollapse = ({
  children,
  isCollapsed,
  rowCount,
  ...otherProps
}: PropsWithChildren<{ isCollapsed: boolean; rowCount: number }>) => {
  const collapseProps = {
    display: "grid",
    gridTemplateColumns: "subgrid",
    gridColumn: "1 / -1",
    maxHeight: rowCount * 32 + 8,
  };
  if (isSafari()) {
    return (
      <Box
        {...collapseProps}
        display={isCollapsed ? "none" : "grid"}
        {...otherProps}
      >
        {children}
      </Box>
    );
  }
  return (
    <Collapse in={!isCollapsed} style={collapseProps} {...otherProps}>
      {children}
    </Collapse>
  );
};
