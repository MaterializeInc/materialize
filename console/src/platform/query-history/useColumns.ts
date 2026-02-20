// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { UseSelectProps } from "downshift";
import { useMemo, useState } from "react";

export const COLUMN_KEYS = [
  "sql",
  "executionId",
  "authenticatedUser",
  "sessionId",
  "finishedStatus",
  "duration",
  "startTime",
  "endTime",
  "rowsReturned",
  "resultSize",
  "clusterName",
  "applicationName",
  "executionStrategy",
  "transactionIsolation",
  "throttledCount",
] as const;

export const COLUMN_MAP: Record<ColumnKey, { label: string }> = {
  applicationName: { label: "Application name" },
  authenticatedUser: { label: "User" },
  clusterName: { label: "Cluster name" },
  duration: { label: "Duration" },
  endTime: { label: "End time" },
  executionId: { label: "Query ID" },
  executionStrategy: { label: "Execution strategy" },
  finishedStatus: { label: "Status" },
  resultSize: { label: "Result size" },
  rowsReturned: { label: "Rows returned" },
  sessionId: { label: "Session ID" },
  sql: { label: "SQL Text" },
  startTime: { label: "Start time" },
  transactionIsolation: { label: "Transaction isolation level" },
  throttledCount: { label: "Throttled count" },
};

export type ColumnKey = (typeof COLUMN_KEYS)[number];

export const DEFAULT_COLUMNS = [
  "sql",
  "executionId",
  "authenticatedUser",
  "finishedStatus",
  "duration",
  "startTime",
  "resultSize",
  "applicationName",
  "throttledCount",
] as ColumnKey[];

export const REQUIRED_COLUMNS = ["sql", "authenticatedUser"] as ColumnKey[];

export type ColumnItem = {
  label: string;
  key: ColumnKey | "*";
};

export const COLUMNS = COLUMN_KEYS.map((key) => ({
  label: COLUMN_MAP[key].label,
  key,
}));

export const ALL_COLUMN_ITEM: ColumnItem = {
  key: "*",
  label: "All columns",
};

/**
 * This is the list of columns that are available to be selected by the user.
 */
export const COLUMN_FILTER_ITEMS = [
  ALL_COLUMN_ITEM,
  ...COLUMNS.filter((column) => !REQUIRED_COLUMNS.includes(column.key)),
];

export const useColumns = ({
  initialColumns,
}: {
  initialColumns: ColumnKey[];
}) => {
  const [selectedColumnItems, setSelectedColumnItems] = useState<ColumnItem[]>(
    COLUMNS.filter((item) => initialColumns.includes(item.key)),
  );

  const orderedSelectedColumnItems = useMemo(
    () =>
      COLUMNS.filter((item) =>
        selectedColumnItems.some(
          (selectedColumnItem) => selectedColumnItem.key === item.key,
        ),
      ),
    [selectedColumnItems],
  );

  const selectedColumnFilterItems = useMemo(
    () =>
      orderedSelectedColumnItems.length === COLUMNS.length
        ? COLUMN_FILTER_ITEMS
        : orderedSelectedColumnItems,
    [orderedSelectedColumnItems],
  );

  const onColumnChange = useMemo<
    UseSelectProps<ColumnItem>["onSelectedItemChange"]
  >(
    () =>
      ({ selectedItem: changedItem }) => {
        if (changedItem.key === ALL_COLUMN_ITEM.key) {
          const allColumnsSelected =
            selectedColumnItems.length === COLUMNS.length;
          if (allColumnsSelected) {
            const requiredColumnItems = REQUIRED_COLUMNS.map((key) => ({
              label: COLUMN_MAP[key].label,
              key,
            }));
            setSelectedColumnItems(requiredColumnItems);
          } else {
            setSelectedColumnItems([...COLUMNS]);
          }

          return;
        }

        const selectedColumnItemsCopy = [...selectedColumnItems];

        const changedItemIndex = selectedColumnItems.indexOf(changedItem);

        const isUnselectOperation = changedItemIndex !== -1;

        if (isUnselectOperation) {
          const isRequiredColumn = REQUIRED_COLUMNS.some(
            (defaultColumn) =>
              defaultColumn === selectedColumnItems[changedItemIndex].key,
          );

          if (isRequiredColumn) {
            return;
          }

          selectedColumnItemsCopy.splice(changedItemIndex, 1);
        } else {
          selectedColumnItemsCopy.push(changedItem);
        }

        setSelectedColumnItems(selectedColumnItemsCopy);
      },
    [setSelectedColumnItems, selectedColumnItems],
  );

  return {
    selectedColumnItems: orderedSelectedColumnItems,
    selectedColumnFilterItems: selectedColumnFilterItems,
    onColumnChange,
  };
};

export type UseColumnsReturn = ReturnType<typeof useColumns>;

export default useColumns;
