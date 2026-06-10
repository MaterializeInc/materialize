// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Text, useTheme } from "@chakra-ui/react";
import React from "react";
import ReactSelect, { GroupBase } from "react-select";

import { useSegment } from "~/analytics/segment";
import { SchemaWithOptionalDatabase as Schema } from "~/api/materialize/schemaList";
import { DropdownIndicator, Option } from "~/components/reactSelectComponents";
import { buildReactSelectFilterStyles, MaterializeTheme } from "~/theme";

export interface SchemaFilterProps {
  schemaList: Schema[] | null;
  selected: Schema | undefined;
  setSelectedSchema: (id: string) => void;
}

const SchemaFilter = ({
  schemaList,
  selected,
  setSelectedSchema,
}: SchemaFilterProps) => {
  const { track } = useSegment();

  const { colors, shadows } = useTheme<MaterializeTheme>();
  if (!schemaList) return null;

  const options: GroupBase<Schema>[] = [
    {
      label: "Filter by schema",
      options: [
        { id: "0", name: "All Schemas", databaseId: "0", databaseName: "" },
        ...schemaList,
      ],
    },
  ];

  return (
    <ReactSelect<Schema, false, GroupBase<Schema>>
      aria-label="Schema filter"
      components={{
        Option: Option,
        DropdownIndicator: DropdownIndicator,
      }}
      isMulti={false}
      isSearchable={false}
      onChange={(value) => {
        if (!value) return;
        setSelectedSchema(value.id);
        track("Schema Filter Changed", { value: value.name });
      }}
      getOptionValue={(option) => option.id.toString()}
      formatOptionLabel={(data) => (
        <>
          {data.databaseName && (
            <Text color={colors.foreground.secondary} as="span">
              {data.databaseName}.
            </Text>
          )}
          {data.name}
        </>
      )}
      options={options}
      value={selected ?? options[0].options[0]}
      styles={buildReactSelectFilterStyles<Schema, false>({
        colors,
        shadows,
      })}
    />
  );
};

export default SchemaFilter;
