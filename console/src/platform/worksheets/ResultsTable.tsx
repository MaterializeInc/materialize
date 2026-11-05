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
  Table,
  Tbody,
  Td,
  Text,
  Th,
  Thead,
  Tr,
  useTheme,
} from "@chakra-ui/react";
import React, { useMemo } from "react";

import { Column } from "~/api/materialize/types";
import formatRows from "~/platform/shell/formatRows";
import { MaterializeTheme } from "~/theme";

/** Long values are cut off here; hovering shows the whole value. */
const MAX_CELL_WIDTH = "60ch";

export interface ResultsTableProps {
  columns: Column[];
  rows: unknown[][];
  /** Grows to its container's height instead of a fixed cap. */
  fill?: boolean;
}

/**
 * A dense results grid with a sticky header and a capped height, so several
 * cells' results fit on screen at once.
 */
export const ResultsTable = ({
  columns,
  rows,
  fill = false,
}: ResultsTableProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const formatted = useMemo(() => formatRows(columns, rows), [columns, rows]);
  return (
    <Box maxHeight={fill ? "100%" : "60"} overflow="auto">
      <Table
        variant="shell"
        size="sm"
        width="auto"
        sx={{
          "th, td": {
            pl: 2,
            pr: 6,
            py: 0.5,
            fontFamily: "mono",
            fontSize: "xs",
            maxWidth: MAX_CELL_WIDTH,
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "pre",
          },
          th: { position: "sticky", top: 0, zIndex: 1, borderTopWidth: 0 },
        }}
      >
        <Thead>
          <Tr>
            {columns.map((c, i) => (
              <Th key={i} title={c.name}>
                {c.name}
              </Th>
            ))}
          </Tr>
        </Thead>
        <Tbody>
          {formatted.length === 0 && (
            <Tr _hover={{}}>
              <Td colSpan={Math.max(columns.length, 1)}>
                <Text as="span" color={colors.foreground.tertiary}>
                  No rows
                </Text>
              </Td>
            </Tr>
          )}
          {formatted.map((row, rowIdx) => (
            <Tr key={rowIdx}>
              {row.map((value, i) =>
                rows[rowIdx][i] === null ? (
                  <Td key={i} color={colors.foreground.tertiary}>
                    NULL
                  </Td>
                ) : (
                  <Td key={i} title={value.length > 60 ? value : undefined}>
                    {value}
                  </Td>
                ),
              )}
            </Tr>
          ))}
        </Tbody>
      </Table>
    </Box>
  );
};
