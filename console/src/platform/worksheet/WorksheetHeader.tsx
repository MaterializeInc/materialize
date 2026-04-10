// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { CloseIcon } from "@chakra-ui/icons";
import { Button, HStack, Spacer, useTheme } from "@chakra-ui/react";
import { useAtom, useAtomValue } from "jotai";
import React, { useMemo } from "react";

import {
  createNamespace,
  isSystemId,
  quoteIdentifier,
} from "~/api/materialize";
import { getSchemaNameFromSearchPath } from "~/api/materialize/useSchemas";
import SchemaSelect, { SchemaOption } from "~/components/SchemaSelect";
import SearchableSelect from "~/components/SearchableSelect/SearchableSelect";
import { useAllClusters } from "~/store/allClusters";
import { useAllSchemas } from "~/store/allSchemas";
import BookOpenIcon from "~/svg/BookOpenIcon";
import { MaterializeTheme } from "~/theme";

import { tutorialVisibleAtom, worksheetSessionAtom } from "./store";

export interface WorksheetHeaderProps {
  /** Executes a SET command to update session parameters (cluster, database, search_path). */
  onRunCommand: (sql: string) => void;
}

/** Builds the SET commands needed to switch to a different schema (and database if changed). */
function createSchemaSelectionCommand(
  newOption: SchemaOption,
  currentOption?: SchemaOption | null,
) {
  let command = "";
  if (
    newOption.databaseName &&
    (!currentOption || newOption.databaseName !== currentOption.databaseName)
  ) {
    command = `SET database = ${quoteIdentifier(newOption.databaseName)}; `;
  }
  command += `SET search_path = ${quoteIdentifier(newOption.name)};`;
  return command;
}

const WorksheetHeader = ({ onRunCommand }: WorksheetHeaderProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const session = useAtomValue(worksheetSessionAtom);
  const [tutorialVisible, setTutorialVisible] = useAtom(tutorialVisibleAtom);

  const {
    data: clusters,
    isError: isClustersError,
    snapshotComplete: isClustersSnapshotComplete,
  } = useAllClusters();

  const {
    data: schemas,
    isError: isSchemasError,
    snapshotComplete: isSchemasSnapshotComplete,
  } = useAllSchemas({ includeSystemSchemas: false });

  const clusterOptions = useMemo(
    () =>
      clusters
        .filter(({ id }) => !isSystemId(id))
        .map(({ name }) => ({ name }))
        .sort((a, b) => a.name.localeCompare(b.name)),
    [clusters],
  );

  const schemaOptions = useMemo(() => {
    if (!schemas) return [];
    return schemas
      .map(({ name, databaseName }) => ({
        id: createNamespace(databaseName, name),
        name,
        databaseName,
      }))
      .sort((a, b) => a.id.localeCompare(b.id));
  }, [schemas]);

  const selectedSchemaOption = useMemo(() => {
    const schemaName = getSchemaNameFromSearchPath(session.searchPath, schemas);
    if (!schemaName || !schemas || !session.database) return null;
    const exists = schemas.some(
      ({ databaseName, name }) =>
        databaseName === session.database && name === schemaName,
    );
    if (!exists) return null;
    return {
      id: createNamespace(session.database, schemaName),
      name: schemaName,
      databaseName: session.database,
    };
  }, [session.searchPath, session.database, schemas]);

  return (
    <HStack
      px="4"
      py="4"
      borderBottomWidth="1px"
      borderColor={colors.border.secondary}
      spacing="3"
    >
      <SearchableSelect<{ name: string }>
        label="Cluster"
        ariaLabel="Cluster"
        isDisabled={isClustersError}
        placeholder="Select one"
        options={clusterOptions}
        onChange={(value) => {
          if (value) {
            onRunCommand(`SET cluster = ${quoteIdentifier(value.name)};`);
          }
        }}
        value={{ name: session.cluster }}
        isLoading={!isClustersSnapshotComplete}
        containerWidth="280px"
        menuWidth="280px"
      />
      <SchemaSelect
        schemas={schemaOptions}
        value={selectedSchemaOption}
        onChange={(option) => {
          if (!option) return;
          const isSame =
            selectedSchemaOption &&
            option.name === selectedSchemaOption.name &&
            option.databaseName === selectedSchemaOption.databaseName;
          if (isSame) return;
          onRunCommand(
            createSchemaSelectionCommand(option, selectedSchemaOption),
          );
        }}
        isDisabled={isSchemasError}
        isLoading={!isSchemasSnapshotComplete}
        placeholder="Select a schema"
        containerWidth="280px"
        menuWidth="280px"
      />
      <Spacer />
      <Button
        flexShrink={0}
        variant="secondary"
        size="sm"
        leftIcon={
          tutorialVisible ? (
            <CloseIcon height="2.5" width="2.5" />
          ) : (
            <BookOpenIcon height="3.5" width="3.5" />
          )
        }
        onClick={() => setTutorialVisible(!tutorialVisible)}
      >
        {tutorialVisible ? "Close Quickstart" : "Quickstart"}
      </Button>
    </HStack>
  );
};

export default WorksheetHeader;
