// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import React from "react";
import { components, createFilter, SingleValueProps } from "react-select";

import { SchemaWithOptionalDatabase as Schema } from "~/api/materialize/schemaList";

import SearchableSelect, {
  SearchableSelectProps,
} from "./SearchableSelect/SearchableSelect";

export type SchemaOption = {
  id: Schema["id"];
  name: Schema["name"];
  databaseName: Schema["databaseName"];
};

export interface SchemaSelectProps
  extends Omit<SearchableSelectProps<SchemaOption>, "ariaLabel" | "options"> {
  schemas: SchemaOption[];
}

/**
 * Creates a map of schemas keyed by its database name
 */
function groupSchemasByDatabaseName(
  schemas: SchemaOption[],
): Map<string, SchemaOption[]> {
  const groups = schemas.reduce((accum, schema) => {
    const { databaseName } = schema;
    const group = accum.get(databaseName);

    if (group) {
      group.push(schema);
    } else {
      accum.set(databaseName, [schema]);
    }

    return accum;
  }, new Map());

  return groups;
}

/**
 * Creates react-select options grouped by database names
 */
function buildSchemaSelectOptions(schemas: SchemaOption[]) {
  const schemasByDatabaseName = groupSchemasByDatabaseName(schemas);
  return Array.from(schemasByDatabaseName, ([key, value]) => ({
    label: key,
    options: value,
  }));
}

const SingleValue = ({
  children,
  ...props
}: SingleValueProps<SchemaOption>) => (
  <components.SingleValue
    {...props}
  >{`${props.data.databaseName}.${props.data.name}`}</components.SingleValue>
);

const SchemaSelect = React.forwardRef(
  ({ schemas, ...props }: SchemaSelectProps, ref: React.Ref<any>) => {
    const schemaSelectOptions = React.useMemo(
      () => buildSchemaSelectOptions(schemas ?? []),
      [schemas],
    );
    return (
      <SearchableSelect<SchemaOption>
        ariaLabel="Select schema"
        ref={ref}
        placeholder="Select one"
        components={{ SingleValue }}
        options={schemaSelectOptions}
        filterOption={createFilter<SchemaOption>({
          stringify: (option) =>
            `${option.data.databaseName}.${option.data.name}`,
        })}
        {...props}
      />
    );
  },
);

export default SchemaSelect;
