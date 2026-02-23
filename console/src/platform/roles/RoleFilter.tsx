// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useTheme } from "@chakra-ui/react";
import React, { useMemo } from "react";
import ReactSelect, { GroupBase } from "react-select";

import { DropdownIndicator, Option } from "~/components/reactSelectComponents";
import { buildReactSelectFilterStyles, MaterializeTheme } from "~/theme";

export const ALL_ROLES_FILTER_ID = "0";

export interface RoleFilterOption {
  id: string;
  label: string;
}

export interface RoleFilterProps {
  roleOptions: RoleFilterOption[];
  selected: RoleFilterOption;
  setSelected: (value: string | undefined) => void;
  containerWidth?: string;
}

export const RoleFilter = ({
  roleOptions,
  selected,
  setSelected,
  containerWidth,
}: RoleFilterProps) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();

  const options: GroupBase<RoleFilterOption>[] = useMemo(
    () => [
      {
        label: "Filter by role",
        options: roleOptions,
      },
    ],
    [roleOptions],
  );

  return (
    <ReactSelect<RoleFilterOption, false, GroupBase<RoleFilterOption>>
      aria-label="Role filter"
      components={{
        Option: Option,
        DropdownIndicator: DropdownIndicator,
      }}
      isMulti={false}
      isSearchable={false}
      getOptionValue={(option) => option.id}
      onChange={(value) => {
        if (!value) return;
        setSelected(value.id);
      }}
      options={options}
      value={selected}
      styles={{
        ...buildReactSelectFilterStyles<RoleFilterOption, false>({
          colors,
          shadows,
        }),
        container: (base) => ({
          ...base,
          width: containerWidth,
        }),
        menu: (base) => ({
          ...base,
          right: 0,
          left: "auto",
        }),
      }}
    />
  );
};
