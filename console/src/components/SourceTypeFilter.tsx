// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import React from "react";
import ReactSelect, { GroupBase } from "react-select";

import { useSegment } from "~/analytics/segment";
import { DropdownIndicator, Option } from "~/components/reactSelectComponents";
import {
  SOURCE_TYPE_OPTIONS,
  SourceTypeFilterProps,
  SourceTypeOption,
} from "~/hooks/useSourceTypeFilter";
import { buildReactSelectFilterStyles, MaterializeTheme } from "~/theme";

export const SourceTypeFilter = ({
  selectedType,
  setSelectedType,
}: SourceTypeFilterProps) => {
  const { track } = useSegment();
  const { colors, shadows } = useTheme<MaterializeTheme>();

  return (
    <ReactSelect<SourceTypeOption, false, GroupBase<SourceTypeOption>>
      aria-label="Source type filter"
      components={{
        Option: Option,
        DropdownIndicator: DropdownIndicator,
      }}
      isMulti={false}
      isSearchable={false}
      onChange={(value) => {
        if (!value) return;
        setSelectedType(value.id);
        track("Source Type Filter Changed", { value: value.name });
      }}
      formatOptionLabel={(option) => option.name}
      getOptionValue={(option) => option.name.toString()}
      options={SOURCE_TYPE_OPTIONS}
      value={selectedType ?? SOURCE_TYPE_OPTIONS[0].options[0]}
      styles={buildReactSelectFilterStyles<SourceTypeOption, false>({
        colors,
        shadows,
      })}
    />
  );
};
