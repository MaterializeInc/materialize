// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { ChevronRightIcon, CloseIcon } from "@chakra-ui/icons";
import { Button, HStack, Spacer, useTheme } from "@chakra-ui/react";
import { useAtom, useAtomValue } from "jotai";
import React, { useMemo } from "react";

import {
  createNamespace,
  isSystemId,
  quoteIdentifier,
} from "~/api/materialize";
import { getSchemaNameFromSearchPath } from "~/api/materialize/useSchemas";
import { SchemaOption } from "~/components/SchemaSelect";
import BreadcrumbPicker from "~/components-v2/BreadcrumbPicker";
import { useAllClusters } from "~/store-v2/allClusters";
import { useAllSchemas } from "~/store-v2/allSchemas";
import BookOpenIcon from "~/svg/BookOpenIcon";
import { MaterializeTheme } from "~/theme";

import { tutorialVisibleAtom, worksheetSessionAtom } from "./store";

export interface WorksheetHeaderProps {
  /** Executes a SET command to update session parameters (cluster, database, search_path). */
  onRunCommand: (sql: string) => void;
}

/** Builds the SET statements needed to switch to a different schema (and database if changed). */
function createSchemaSelectionCommands(
  newOption: SchemaOption,
  currentOption?: SchemaOption | null,
): string[] {
  const statements: string[] = [];
  if (
    newOption.databaseName &&
    (!currentOption || newOption.databaseName !== currentOption.databaseName)
  ) {
    statements.push(
      `SET database = ${quoteIdentifier(newOption.databaseName)};`,
    );
  }
  statements.push(`SET search_path = ${quoteIdentifier(newOption.name)};`);
  return statements;
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

  const clusterItems = useMemo(
    () =>
      clusters
        .filter(({ id }) => !isSystemId(id))
        .map(({ name }) => ({ id: name, label: name }))
        .sort((a, b) => a.label.localeCompare(b.label)),
    [clusters],
  );

  const schemaOptionsById = useMemo(() => {
    const map = new Map<string, SchemaOption>();
    if (!schemas) return map;
    for (const { name, databaseName } of schemas) {
      const id = createNamespace(databaseName, name);
      map.set(id, { id, name, databaseName });
    }
    return map;
  }, [schemas]);

  const schemaItems = useMemo(() => {
    const currentDb = session.database;
    return Array.from(schemaOptionsById.values())
      .map((option) => ({
        id: option.id,
        label: option.databaseName
          ? `${option.databaseName}.${option.name}`
          : option.name,
        group: option.databaseName ?? undefined,
      }))
      .sort((a, b) => {
        const aCurrent = a.group === currentDb;
        const bCurrent = b.group === currentDb;
        if (aCurrent !== bCurrent) return aCurrent ? -1 : 1;
        return a.id.localeCompare(b.id);
      });
  }, [schemaOptionsById, session.database]);

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

  const schemaTrigger = selectedSchemaOption
    ? `${selectedSchemaOption.databaseName}.${selectedSchemaOption.name}`
    : "Select schema";

  return (
    <HStack
      px="4"
      py="3"
      borderBottomWidth="1px"
      borderColor={colors.border.secondary}
      spacing="1"
    >
      <BreadcrumbPicker
        trigger={session.cluster || "Select cluster"}
        ariaLabel="Cluster"
        items={clusterItems}
        selectedId={session.cluster}
        onSelect={(item) => {
          if (item.id === session.cluster) return;
          onRunCommand(`SET cluster = ${quoteIdentifier(item.id)};`);
        }}
        isLoading={!isClustersSnapshotComplete}
        isDisabled={isClustersError}
        searchPlaceholder="Search clusters…"
      />
      <ChevronRightIcon
        boxSize="3"
        color={colors.foreground.tertiary}
        aria-hidden
      />
      <BreadcrumbPicker
        trigger={schemaTrigger}
        ariaLabel="Schema"
        items={schemaItems}
        selectedId={selectedSchemaOption?.id}
        onSelect={(item) => {
          const newOption = schemaOptionsById.get(item.id);
          if (!newOption) return;
          if (
            selectedSchemaOption &&
            newOption.name === selectedSchemaOption.name &&
            newOption.databaseName === selectedSchemaOption.databaseName
          ) {
            return;
          }
          for (const stmt of createSchemaSelectionCommands(
            newOption,
            selectedSchemaOption,
          )) {
            onRunCommand(stmt);
          }
        }}
        isLoading={!isSchemasSnapshotComplete}
        isDisabled={isSchemasError}
        searchPlaceholder="Search schemas…"
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
