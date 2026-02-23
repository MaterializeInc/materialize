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
import { Database } from "~/api/materialize/databaseList";
import { DropdownIndicator, Option } from "~/components/reactSelectComponents";
import { buildReactSelectFilterStyles, MaterializeTheme } from "~/theme";

export interface DatabaseFilterProps {
  databaseList?: Database[];
  selected: Database | undefined;
  setSelectedDatabase: (id: string) => void;
}

const DatabaseFilter = ({
  databaseList,
  selected,
  setSelectedDatabase,
}: DatabaseFilterProps) => {
  const { track } = useSegment();

  const { colors, shadows } = useTheme<MaterializeTheme>();
  if (!databaseList) return null;

  const options: GroupBase<Database>[] = [
    {
      label: "Filter by database",
      options: [{ id: "0", name: "All Databases" }, ...databaseList],
    },
  ];
  return (
    <ReactSelect<Database, false, GroupBase<Database>>
      aria-label="Database filter"
      components={{
        Option: Option,
        DropdownIndicator: DropdownIndicator<Database, false>,
      }}
      isMulti={false}
      isSearchable={false}
      onChange={(value) => {
        if (!value) return;
        setSelectedDatabase(value.id);
        track("Database Filter Changed", { value: value.name });
      }}
      getOptionValue={(option) => option.id.toString()}
      formatOptionLabel={(data) => data.name}
      options={options}
      value={selected ?? options[0].options[0]}
      styles={buildReactSelectFilterStyles<Database, false>({
        colors,
        shadows,
      })}
    />
  );
};

export default DatabaseFilter;
