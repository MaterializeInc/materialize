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
import { HealthStatus } from "~/platform/connectors/utils";
import { buildReactSelectFilterStyles, MaterializeTheme } from "~/theme";

export interface HealthStatusOption {
  id: HealthStatus;
  label: string;
}

export interface HealthFilterProps {
  selected: HealthStatus;
  setSelected: (value: HealthStatus) => void;
}

const options: HealthStatusOption[] = [
  { id: "unhealthy", label: "Unhealthy" },
  { id: "healthy", label: "Healthy" },
  { id: "paused", label: "Paused" },
  { id: "all", label: "All Statuses" },
];

export const ConnectorHealthFilter = ({
  selected,
  setSelected,
}: HealthFilterProps) => {
  const { track } = useSegment();

  const { colors, shadows } = useTheme<MaterializeTheme>();

  const groups: GroupBase<HealthStatusOption>[] = [
    {
      label: "Filter by status",
      options,
    },
  ];

  return (
    <ReactSelect<HealthStatusOption, false, GroupBase<HealthStatusOption>>
      aria-label="Health filter"
      components={{
        Option: Option,
        DropdownIndicator: DropdownIndicator,
      }}
      isMulti={false}
      isSearchable={false}
      getOptionValue={(option) => option.id.toString()}
      onChange={(value) => {
        if (!value) return;
        setSelected(value.id);
        track("Health Filter Changed", { value });
      }}
      options={groups}
      value={options.find((o) => o.id === selected) ?? options[0]}
      styles={buildReactSelectFilterStyles<HealthStatusOption, false>({
        colors,
        shadows,
      })}
    />
  );
};
