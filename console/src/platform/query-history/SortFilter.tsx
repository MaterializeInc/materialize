// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import React, { RefObject } from "react";
import { useController, UseFormHandleSubmit } from "react-hook-form";

import { QueryHistoryListSchema } from "~/api/materialize/query-history/queryHistoryList";
import SearchableSelect, {
  SelectOptionKind,
} from "~/components/SearchableSelect/SearchableSelect";
import { buildSearchableSelectFilterStyles } from "~/components/SearchableSelect/utils";
import SortFilterMenuBottom from "~/components/SortFilterMenuBottom";
import SortIcon from "~/svg/SortIcon";
import { MaterializeTheme } from "~/theme";

import { getQueryHistorySelectStyleOverrides } from "./queryHistoryUtils";
import { useReactSelectForceFocus } from "./useReactSelectForceFocus";

type SortFilterProps = {
  submitForm: ReturnType<UseFormHandleSubmit<QueryHistoryListSchema>>;
};

const SORT_FILTER_COLUMNS = [
  {
    id: "start_time",
    name: "Start time",
  },
  {
    id: "end_time",
    name: "End time",
  },
  {
    id: "duration",
    name: "Duration",
  },
  {
    id: "status",
    name: "Status",
  },
  {
    id: "resultSize",
    name: "Result size",
  },
];

const SORT_FILTER_OPTIONS = [
  {
    label: "Sort by",
    options: SORT_FILTER_COLUMNS,
  },
];

const SortFilter = ({ submitForm }: SortFilterProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  const { field: sortOrderField } = useController<
    QueryHistoryListSchema,
    "sortOrder"
  >({
    name: "sortOrder",
  });

  const { field: sortField } = useController<
    QueryHistoryListSchema,
    "sortField"
  >({
    name: "sortField",
  });

  const { ref, onMenuOpen } = useReactSelectForceFocus();

  const handleOptionChange = (option: SelectOptionKind | null) => {
    if (!option || !("id" in option)) return;
    sortField.onChange(option.id);
    submitForm();
  };

  const currentValue = SORT_FILTER_COLUMNS.find(
    (column) => column.id === sortField.value,
  );

  return (
    <SearchableSelect<SelectOptionKind, false>
      ariaLabel="Sort filter"
      ref={ref as RefObject<any>}
      onMenuOpen={onMenuOpen}
      styles={buildSearchableSelectFilterStyles(
        {
          colors,
          shadows,
          containerWidth: "144px",
          menuWidth: "240px",
        },
        getQueryHistorySelectStyleOverrides({ colors, shadows }),
      )}
      leftIcon={<SortIcon />}
      isSearchable={false}
      options={SORT_FILTER_OPTIONS}
      value={currentValue}
      onChange={(value) => handleOptionChange(value)}
      renderMenuBottom={(renderMenuBottomProps) => {
        const isAscendingSortOrder = sortOrderField.value === "asc";
        const handleButtonClick = () => {
          if (isAscendingSortOrder) {
            sortOrderField.onChange("desc");
          } else {
            sortOrderField.onChange("asc");
          }

          submitForm();
          renderMenuBottomProps.selectProps.onMenuClose();
        };

        return (
          <SortFilterMenuBottom
            ascendingButtonProps={{
              onClick: handleButtonClick,
              color: isAscendingSortOrder
                ? colors.accent.brightPurple
                : undefined,
            }}
            descendingButtonProps={{
              onClick: handleButtonClick,
              color: !isAscendingSortOrder
                ? colors.accent.brightPurple
                : undefined,
            }}
          />
        );
      }}
    />
  );
};

export default SortFilter;
