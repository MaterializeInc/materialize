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
  Button,
  GridItem,
  HStack,
  Text,
  useTheme,
  VStack,
} from "@chakra-ui/react";
import * as Sentry from "@sentry/react";
import { useAtom } from "jotai";
import React, { useMemo } from "react";

import {
  createNamespace,
  isSystemId,
  quoteIdentifier,
} from "~/api/materialize";
import { SUBSCRIBE_ERROR_CODE } from "~/api/materialize/SubscribeManager";
import SchemaSelect, { SchemaOption } from "~/components/SchemaSelect";
import { useAllClusters } from "~/store/allClusters";
import { useAllSchemas } from "~/store/allSchemas";
import BookOpenIcon from "~/svg/BookOpenIcon";
import { MaterializeTheme } from "~/theme";

import ClusterDropdown from "./ClusterDropdown";
import { NAVBAR_HEIGHT_PX } from "./constants";
import { setStoredSidebarVisibility, shellStateAtom } from "./store/shell";
import { getSelectedSchemaOption } from "./utils";

function createSchemaOptionSelectionCommand(
  newSchemaOption: SchemaOption,
  currentSchemaOption?: SchemaOption | null,
) {
  let command = "";

  if (
    newSchemaOption.databaseName &&
    (!currentSchemaOption ||
      newSchemaOption.databaseName !== currentSchemaOption.databaseName)
  ) {
    command = `SET database = ${quoteIdentifier(newSchemaOption.databaseName)}; `;
  }

  command += `SET search_path = ${quoteIdentifier(newSchemaOption.name)};`;

  return command;
}

const ShellHeader = ({
  runCommand,
}: {
  runCommand: (command: string) => void;
}) => {
  const { colors, shadows } = useTheme<MaterializeTheme>();
  const [shellState, setShellState] = useAtom(shellStateAtom);
  const {
    sessionParameters: { database, search_path: searchPath, cluster },
  } = shellState;
  const {
    data: schemas,
    snapshotComplete: isSchemasSnapshotComplete,
    isError: isSchemasError,
  } = useAllSchemas({ includeSystemSchemas: false });

  const {
    data: clusters,
    isError: isClustersError,
    error: clustersSubscribeError,
    snapshotComplete: isClustersSnapshotComplete,
  } = useAllClusters();

  if (
    isClustersError &&
    clustersSubscribeError &&
    clustersSubscribeError.code !== SUBSCRIBE_ERROR_CODE.CONNECTION_CLOSED
  ) {
    Sentry.captureException(
      new Error(
        `ClusterDropdown load errors: ${clustersSubscribeError.message} (${clustersSubscribeError.code})`,
      ),
    );
  }
  const isSelectedClusterExtant = clusters.some((c) => c.name === cluster);

  const clusterOptions = useMemo(() => {
    const allClusters = clusters
      // Don't show system clusters in the dropdown
      .filter(({ id }) => !isSystemId(id))
      .map((c) => ({
        name: c.name,
      }));

    allClusters.sort();
    return allClusters;
  }, [clusters]);

  const selectedSchemaOption = useMemo(() => {
    return getSelectedSchemaOption(searchPath, database, schemas);
  }, [searchPath, database, schemas]);

  const schemaOptions = useMemo(() => {
    if (schemas === undefined) {
      return [];
    }

    const newSchemaOptions = schemas.map(({ name, databaseName }) => ({
      id: createNamespace(databaseName, name),
      name,
      databaseName,
    }));

    newSchemaOptions.sort((a, b) => a.id.localeCompare(b.id));

    return newSchemaOptions;
  }, [schemas]);

  const handleSchemaSelect = (newSchemaOption: SchemaOption | null) => {
    if (!newSchemaOption) {
      return;
    }
    const isSelectingCurrentOption =
      selectedSchemaOption &&
      newSchemaOption.name === selectedSchemaOption.name &&
      newSchemaOption.databaseName === selectedSchemaOption.databaseName;

    if (isSelectingCurrentOption) {
      return;
    }

    const command = createSchemaOptionSelectionCommand(
      newSchemaOption,
      selectedSchemaOption,
    );

    runCommand(command);
  };

  const setTutorialVisibility = (tutorialVisible: boolean) => {
    setShellState((prevState) => ({ ...prevState, tutorialVisible }));
    setStoredSidebarVisibility(tutorialVisible);
  };

  return (
    <GridItem area="header" display="flex">
      <VStack spacing="0" width="100%">
        <HStack
          padding="4"
          borderBottomWidth="1px"
          borderColor={colors.border.secondary}
          justifyContent="space-between"
          alignItems="center"
          width="100%"
          maxHeight={NAVBAR_HEIGHT_PX}
        >
          <HStack>
            <ClusterDropdown
              onChange={(clusterName: string) => {
                runCommand(`SET cluster = ${quoteIdentifier(clusterName)};`);
              }}
              options={clusterOptions}
              value={cluster ?? ""}
              isDisabled={isClustersError}
              isLoading={!isClustersSnapshotComplete}
            />

            <SchemaSelect
              schemas={schemaOptions}
              value={selectedSchemaOption}
              onChange={handleSchemaSelect}
              isDisabled={isSchemasError}
              isLoading={!isSchemasSnapshotComplete}
              placeholder="Select a schema"
              containerWidth="280px"
              menuWidth="280px"
            />
          </HStack>
          <Button
            flexShrink="0"
            variant="secondary"
            aria-label="Tutorial button"
            title={
              shellState.tutorialVisible
                ? "Close Quickstart"
                : "Open Quickstart"
            }
            leftIcon={
              shellState.tutorialVisible ? (
                <CloseIcon height="3" width="3" />
              ) : (
                <BookOpenIcon />
              )
            }
            borderRadius="3xl"
            size="sm"
            _focus={{
              border: `1px solid ${colors.accent.brightPurple}`,
              boxShadow: shadows.input.focus,
            }}
            onClick={() => setTutorialVisibility(!shellState.tutorialVisible)}
          >
            {shellState.tutorialVisible ? "Close Quickstart" : "Quickstart"}
          </Button>
        </HStack>
        {!isClustersError &&
          // Need to make sure the cluster has loaded before checking if it exists
          cluster !== undefined &&
          isClustersSnapshotComplete &&
          !isSelectedClusterExtant && (
            <HStack
              width="100%"
              py="2"
              px="3"
              borderWidth="1px"
              borderColor={colors.border.warn}
              backgroundColor={colors.background.warn}
            >
              <HStack spacing="3">
                <Text
                  color={colors.foreground.primary}
                  textStyle="text-ui-med"
                  pr="3"
                  borderRightColor={colors.border.warn}
                  borderRightWidth="1px"
                >
                  Warning
                </Text>
                <Text color={colors.foreground.primary} textStyle="text-ui-reg">
                  The currently selected cluster does not exist.
                </Text>
              </HStack>
            </HStack>
          )}
      </VStack>
    </GridItem>
  );
};

export default ShellHeader;
