// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Button, HStack, useDisclosure } from "@chakra-ui/react";
import React from "react";
import { ControllerRenderProps, FieldValues } from "react-hook-form";

import { mapRowToObject } from "~/api/materialize";
import {
  ConnectionFiltered,
  useConnectionsFiltered,
} from "~/api/materialize/connection/useConnections";
import SearchableSelect, {
  SearchableSelectProps,
} from "~/components/SearchableSelect/SearchableSelect";

import NewCsrConnectionModal from "./NewCsrConnectionModal";

const CsrSelectionControl = <T extends FieldValues = FieldValues>({
  selectField,
  variant,
  selectProps,
}: {
  selectField: ControllerRenderProps<T, any>;
  variant?: "default" | "error";
  selectProps?: Partial<SearchableSelectProps>;
}) => {
  const {
    isOpen: isModalOpen,
    onOpen: onModalOpen,
    onClose: onCloseModal,
  } = useDisclosure();
  const {
    loading: isLoading,
    results: csrConnections,
    refetch,
    failedToLoad,
  } = useConnectionsFiltered({
    type: "confluent-schema-registry",
  });

  const onClose = async (connectionId?: string) => {
    onCloseModal();
    if (!connectionId) {
      return;
    }
    const [refetched] = (await refetch()) ?? [];
    if (failedToLoad) {
      return;
    }
    const created = refetched?.rows
      .map(
        (row) => mapRowToObject(row, refetched.columns) as ConnectionFiltered,
      )
      .find((conn) => conn.id === connectionId);
    if (created) {
      selectField.onChange(created);
    }
  };

  return (
    <HStack gap={4}>
      <SearchableSelect
        ariaLabel="Choose connection"
        placeholder="Choose connection"
        options={csrConnections ?? []}
        variant={variant}
        containerWidth="100%"
        isDisabled={isLoading}
        {...selectProps}
        {...selectField}
      />
      <Box>or</Box>
      <Box>
        <Button size="sm" onClick={onModalOpen}>
          Create a new connection
        </Button>
        <NewCsrConnectionModal
          isOpen={isModalOpen}
          onClose={onClose}
          onSuccess={onClose}
        />
      </Box>
    </HStack>
  );
};

export default CsrSelectionControl;
