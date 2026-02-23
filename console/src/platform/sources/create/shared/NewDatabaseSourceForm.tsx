// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CloseIcon } from "@chakra-ui/icons";
import {
  Box,
  Button,
  FormControl,
  HStack,
  Input,
  Switch,
  VStack,
} from "@chakra-ui/react";
import React from "react";
import { useController, useFieldArray, UseFormReturn } from "react-hook-form";

import useConnectorClusters from "~/api/materialize/cluster/useConnectorClusters";
import { Database } from "~/api/materialize/databaseList";
import { Schema } from "~/api/materialize/schemaList";
import { Cluster } from "~/api/materialize/types";
import useSchemas from "~/api/materialize/useSchemas";
import Alert from "~/components/Alert";
import ErrorBox from "~/components/ErrorBox";
import {
  FORM_CONTENT_WIDTH,
  FORM_LABEL_LINE_HEIGHT,
  FormContainer,
  FormSection,
  LabeledInput,
} from "~/components/formComponentsV2";
import ObjectNameInput from "~/components/ObjectNameInput";
import SchemaSelect from "~/components/SchemaSelect";
import SearchableSelect from "~/components/SearchableSelect/SearchableSelect";

import { DatabaseTypeProp } from "./constants";

type TableFields = {
  name: string;
  alias: string;
  schemaName?: string;
};

export type DatabaseSourceFormState = {
  name: string;
  database: Database | null;
  schema: Schema | null;
  cluster: Cluster | null;
  publication: string;
  allTables: boolean;
  tables: TableFields[];
};

type FormProp = {
  form: UseFormReturn<DatabaseSourceFormState>;
};

const GeneralSection = ({
  databaseType,
  form,
  schemas,
  clusters,
}: { schemas: Schema[]; clusters: Cluster[] } & DatabaseTypeProp &
  FormProp) => {
  const { control, formState, register } = form;
  const { field: schemaField } = useController({
    control,
    name: "schema",
    rules: { required: "Schema is required." },
  });

  const { field: clusterField } = useController({
    control,
    name: "cluster",
    rules: {
      required: "Cluster is required.",
    },
  });
  return (
    <FormSection title="General" caption="Step 1">
      <FormControl isInvalid={!!formState.errors.name} isRequired>
        <LabeledInput
          label="Name"
          message="Alphanumeric characters and underscores only."
          error={formState.errors.name?.message}
        >
          <ObjectNameInput
            {...register("name", {
              required: "Source name is required.",
            })}
            placeholder={`my_${databaseType}_source`}
            variant={formState.errors.name ? "error" : "default"}
            autoFocus
          />
        </LabeledInput>
      </FormControl>
      <FormControl
        isInvalid={!!formState.errors.schema}
        data-testid="connection-schema"
        isRequired
      >
        <LabeledInput
          label="Schema"
          message="The database and schema where this source will be created."
          error={formState.errors.schema?.message}
        >
          <SchemaSelect
            {...schemaField}
            schemas={schemas}
            variant={formState.errors.schema ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
      <FormControl
        isInvalid={!!formState.errors.cluster}
        data-testid="cluster-selector"
        isRequired
      >
        <LabeledInput
          label="Cluster"
          error={formState.errors.cluster?.message}
          message="The cluster to maintain this source."
        >
          <SearchableSelect
            ariaLabel="Select cluster"
            placeholder="Select one"
            {...clusterField}
            options={[{ label: "Select cluster", options: clusters }]}
            variant={formState.errors.cluster ? "error" : "default"}
          />
        </LabeledInput>
      </FormControl>
    </FormSection>
  );
};

const TABLE_INPUT_LABEL_MARGIN = 2;

export const TableNameArrayInput = ({
  databaseType,
  form,
}: DatabaseTypeProp & FormProp) => {
  const { formState, control, getValues, register, trigger, watch } = form;
  const { append, fields, remove } = useFieldArray({
    control,
    name: "tables",
  });
  const watchTables = watch("tables");

  // react-hook-form recommends the validity for an object be bound to the
  // first field in the control sequence. Since the first object differs
  // between MySQL and PostgreSQL, handle the special case.
  const isInvalid = (fieldName: keyof TableFields, index: number) => {
    const field = formState.errors.tables?.[index];
    if (
      ((databaseType === "mysql" || databaseType === "sql-server") &&
        fieldName === "schemaName") ||
      (databaseType === "postgres" && fieldName === "name")
    ) {
      return !!field;
    }
    return !!(field && field[fieldName]);
  };

  const isUniqueName = (thisTable: TableFields, thatTable: TableFields) => {
    if (thisTable.schemaName !== thatTable.schemaName) {
      return true;
    }
    return thisTable.name !== thatTable.name;
  };

  const hasEmptyFields = watchTables.some((t) => t.name === "");
  return (
    <VStack alignItems="flex-start" gap={4} width={FORM_CONTENT_WIDTH}>
      {fields.map((field, index) => (
        <HStack key={field.id} alignItems="flex-start" gap={4} width="100%">
          {/* TODO (#3400): Combine schema name and table name using mz_source_references */}
          {(databaseType === "mysql" || databaseType === "sql-server") && (
            <FormControl isInvalid={isInvalid("schemaName", index)} isRequired>
              <LabeledInput
                label="Schema name"
                error={formState.errors.tables?.[index]?.schemaName?.message}
                inputBoxProps={{ mt: TABLE_INPUT_LABEL_MARGIN }}
              >
                <Input
                  {...register(`tables.${index}.schemaName` as const, {
                    onChange: () => {
                      trigger(`tables.${index}.name`);
                    },
                    required: "Schema name is required.",
                    setValueAs: (value: string) => value.trim(),
                  })}
                  placeholder="schema name"
                  autoCorrect="off"
                  spellCheck="false"
                  size="sm"
                  variant={
                    formState.errors.tables?.[index]?.schemaName
                      ? "error"
                      : "default"
                  }
                />
              </LabeledInput>
            </FormControl>
          )}
          <FormControl isInvalid={isInvalid("name", index)} isRequired>
            <LabeledInput
              label="Table name"
              error={formState.errors.tables?.[index]?.name?.message}
              inputBoxProps={{ mt: TABLE_INPUT_LABEL_MARGIN }}
            >
              <Input
                {...register(`tables.${index}.name` as const, {
                  onChange: () => {
                    for (let i = 0; i < fields.length; i++) {
                      trigger(`tables.${index}.name`);
                    }
                  },
                  required: "Table name is required.",
                  validate: {
                    unique: () => {
                      const tables = getValues("tables");
                      const thisTable = tables[index];
                      const count = tables.filter(
                        (thatTable) => !isUniqueName(thisTable, thatTable),
                      ).length;
                      return count <= 1 || "Table names must be unique.";
                    },
                  },
                  setValueAs: (value: string) => value.trim(),
                })}
                placeholder="table name"
                autoCorrect="off"
                spellCheck="false"
                size="sm"
                variant={
                  formState.errors.tables?.[index]?.name ? "error" : "default"
                }
              />
            </LabeledInput>
          </FormControl>
          <FormControl isInvalid={isInvalid("alias", index)}>
            <LabeledInput
              label="Alias"
              error={formState.errors.tables?.[index]?.alias?.message}
              inputBoxProps={{ mt: TABLE_INPUT_LABEL_MARGIN }}
            >
              <Input
                {...register(`tables.${index}.alias` as const, {
                  onChange: () => {
                    for (let i = 0; i < fields.length; i++) {
                      trigger(`tables.${i}.alias`);
                    }
                  },
                  validate: {
                    unique: (value) => {
                      if (!value) return true;
                      const count = getValues("tables")
                        .map((row) => row.alias)
                        .filter((alias) => alias === value).length;
                      return count <= 1 || "Aliases must be unique.";
                    },
                  },
                  setValueAs: (value: string) => value.trim(),
                })}
                placeholder="alias"
                autoCorrect="off"
                spellCheck="false"
                size="sm"
                variant={
                  formState.errors.tables?.[index]?.alias ? "error" : "default"
                }
              />
            </LabeledInput>
          </FormControl>
          <Box
            alignSelf="flex-start"
            marginTop={TABLE_INPUT_LABEL_MARGIN + FORM_LABEL_LINE_HEIGHT}
          >
            <Button
              variant="borderless"
              height="8"
              minWidth="8"
              width="8"
              onClick={() => remove(index)}
              visibility={index > 0 ? "visible" : "hidden"}
              isDisabled={index === 0}
              title="Remove table"
            >
              <CloseIcon height="8px" width="8px" />
            </Button>
          </Box>
        </HStack>
      ))}
      <Box>
        <Button
          size="sm"
          onClick={() =>
            append({
              name: "",
              alias: "",
              schemaName:
                databaseType === "mysql" || databaseType === "sql-server"
                  ? ""
                  : undefined,
            })
          }
          mt={4}
          isDisabled={!!formState.errors.tables || hasEmptyFields}
        >
          Add table
        </Button>
      </Box>
    </VStack>
  );
};

const ConfigurationSection = ({
  databaseType,
  form,
}: DatabaseTypeProp & FormProp) => {
  const { formState, register, watch } = form;
  const watchAllTables = watch("allTables");
  return (
    <FormSection title="Configuration" caption="Step 2">
      {databaseType === "postgres" && (
        <FormControl isInvalid={!!formState.errors.publication} isRequired>
          <LabeledInput
            label="Publication"
            error={formState.errors.publication?.message}
          >
            <ObjectNameInput
              {...register("publication", {
                required: "Publication is required.",
              })}
              placeholder="mz_source"
              autoCorrect="off"
              variant={formState.errors.publication ? "error" : "default"}
            />
          </LabeledInput>
        </FormControl>
      )}
      <FormControl>
        <HStack>
          <LabeledInput label="For all tables" inputBoxProps={{ mt: 0 }}>
            <Switch {...register("allTables")} />
          </LabeledInput>
        </HStack>
      </FormControl>
      {!watchAllTables && (
        <TableNameArrayInput form={form} databaseType={databaseType} />
      )}
    </FormSection>
  );
};

const NewDatabaseSourceForm = ({
  form,
  databaseType,
  generalFormError,
}: {
  generalFormError?: string;
} & DatabaseTypeProp &
  FormProp) => {
  const { results: schemas, failedToLoad: schemasFailedToLoad } = useSchemas({
    filterByCreatePrivilege: true,
  });
  const { results: clusters, failedToLoad: clustersFailedToLoad } =
    useConnectorClusters();

  const loadingError = schemasFailedToLoad || clustersFailedToLoad;

  if (loadingError) {
    return <ErrorBox />;
  }
  return (
    <FormContainer>
      <GeneralSection
        form={form}
        databaseType={databaseType}
        schemas={schemas ?? []}
        clusters={clusters ?? []}
      />
      <ConfigurationSection form={form} databaseType={databaseType} />
      {generalFormError && (
        <Alert variant="error" minWidth="100%" message={generalFormError} />
      )}
    </FormContainer>
  );
};

export default NewDatabaseSourceForm;
