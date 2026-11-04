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
  HStack,
  IconButton,
  Select,
  Text,
  useTheme,
} from "@chakra-ui/react";
import React, { useEffect, useState } from "react";

import { ChevronDownIcon, ChevronRightIcon, CloseIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

import { chartTitle, selectWidth } from "./cellLabels";
import { ChartView } from "./ChartView";
import { ChartSpec, WorksheetCell } from "./store";
import { PublishedRows, WorksheetRuntime } from "./worksheetRuntime";

export interface ChartCellProps {
  dragHandle?: React.ReactNode;
  onRemove: () => void;
  cell: WorksheetCell & { chart: ChartSpec };
  runtime: WorksheetRuntime;
  sqlCellNames: string[];
  onChange: (update: Partial<WorksheetCell>) => void;
  /** Hides the controls, leaving a title and the chart, as on a dashboard. */
  presenting?: boolean;
}

/**
 * Plots another cell's rows, re-rendering whenever that cell republishes.
 * Worksheets saved before cells could show their own chart have these.
 */
export const ChartCell = ({
  cell,
  runtime,
  sqlCellNames,
  onChange,
  presenting = false,
  onRemove,
  dragHandle,
}: ChartCellProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const { chart } = cell;
  // Tagged with its source, so a source change never shows, or picks axes
  // from, the previous source's rows.
  const [published, setPublished] = useState<{
    source: string;
    value: PublishedRows;
  }>();

  useEffect(() => {
    setPublished(undefined);
    if (!chart.source) return;
    const source = chart.source;
    return runtime.observe(source, {
      fulfilled: (value) =>
        setPublished(
          value && Array.isArray(value.rows) ? { source, value } : undefined,
        ),
      rejected: () => setPublished(undefined),
    });
  }, [runtime, chart.source]);

  const current =
    published?.source === chart.source ? published.value : undefined;
  const setChart = (update: Partial<ChartSpec>) =>
    onChange({ chart: { ...chart, ...update } });

  const unavailableMessage = !chart.source
    ? "Choose a cell to chart"
    : !sqlCellNames.includes(chart.source)
      ? `${chart.source} no longer exists. Choose another cell to chart.`
      : `Run ${chart.source} to see this chart`;

  return (
    <Box
      minW="0"
      {...(presenting
        ? { borderBottomWidth: "1px", borderColor: colors.border.primary }
        : {
            borderLeftWidth: "3px",
            borderColor: colors.border.primary,
            pl: "1",
          })}
    >
      {presenting && (
        <HStack px="2" py="1.5">
          <Text textStyle="text-ui-med" fontWeight="600" noOfLines={1}>
            {chartTitle(chart)}
          </Text>
        </HStack>
      )}
      <ChartView
        chart={chart}
        rows={current?.rows}
        totalRows={current?.totalRows}
        onChange={setChart}
        collapsed={Boolean(cell.collapsed)}
        showControls={!presenting}
        unavailableMessage={unavailableMessage}
        leading={
          <>
            <IconButton
              aria-label={cell.collapsed ? "Expand chart" : "Collapse chart"}
              title={cell.collapsed ? "Expand" : "Collapse"}
              icon={cell.collapsed ? <ChevronRightIcon /> : <ChevronDownIcon />}
              size="xs"
              variant="inline"
              minW="5"
              onClick={() => onChange({ collapsed: !cell.collapsed })}
            />
            <Select
              size="xs"
              flexShrink={0}
              width={selectWidth([chart.source, ...sqlCellNames])}
              fontFamily="mono"
              aria-label="Source cell"
              title="The cell to chart"
              value={chart.source}
              // A new source has different columns, so the axes start over.
              onChange={(e) =>
                setChart({ source: e.target.value, x: "", y: "", color: "" })
              }
            >
              {!sqlCellNames.includes(chart.source) && (
                <option value={chart.source}>{chart.source || "–"}</option>
              )}
              {Array.from(new Set(sqlCellNames)).map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </Select>
          </>
        }
        trailing={
          <>
            {dragHandle}
            <IconButton
              aria-label="Delete cell"
              title="Delete cell"
              icon={<CloseIcon stroke="currentColor" boxSize="3" />}
              size="xs"
              variant="inline"
              minW="5"
              onClick={onRemove}
            />
          </>
        }
      />
    </Box>
  );
};
