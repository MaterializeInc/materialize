// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { SearchIcon } from "@chakra-ui/icons";
import {
  Badge,
  Box,
  HStack,
  Input,
  InputGroup,
  InputLeftElement,
  List,
  ListItem,
  Text,
  useTheme,
} from "@chakra-ui/react";
import { useCombobox } from "downshift";
import React from "react";

import { MaterializeTheme } from "~/theme";

import { CatalogRelation } from "./dataProductGraph";
import {
  OntologyTableCandidate,
  rankOntologyTables,
} from "./ontologyTableChoice";

export interface OntologyTablePickerProps {
  /** The chosen table's name, as typed or picked. */
  value: string;
  onChange: (name: string) => void;
  relations: CatalogRelation[];
  /** Ids of relations shaped like an ontology table, offered first. */
  shapeIds: ReadonlySet<string>;
}

/**
 * Picks the table declared joins are read from, by fuzzy search over the
 * environment's relations. Enter on text that matches no suggestion keeps the
 * text, so a table that does not exist yet can be named.
 */
export const OntologyTablePicker = ({
  value,
  onChange,
  relations,
  shapeIds,
}: OntologyTablePickerProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const [inputValue, setInputValue] = React.useState(value);
  React.useEffect(() => setInputValue(value), [value]);

  const candidates = React.useMemo(
    () =>
      rankOntologyTables(
        inputValue === value ? "" : inputValue,
        relations,
        shapeIds,
      ),
    [inputValue, value, relations, shapeIds],
  );

  const {
    isOpen,
    getMenuProps,
    getInputProps,
    getItemProps,
    highlightedIndex,
  } = useCombobox<OntologyTableCandidate>({
    items: candidates,
    inputValue,
    itemToString: (item) => item?.name ?? "",
    onInputValueChange: ({ inputValue: next }) => setInputValue(next ?? ""),
    onSelectedItemChange: ({ selectedItem }) => {
      if (selectedItem) onChange(selectedItem.name);
    },
    // Leaving the box without choosing shows the chosen table again.
    stateReducer: (_state, { type, changes }) =>
      type === useCombobox.stateChangeTypes.InputBlur
        ? { ...changes, inputValue: value }
        : changes,
  });

  const showMenu = isOpen && candidates.length > 0;

  return (
    <HStack spacing={2}>
      <Text
        as="label"
        textStyle="text-ui-reg"
        color={colors.foreground.secondary}
        whiteSpace="nowrap"
        htmlFor="ontology-table-picker"
      >
        Ontology table
      </Text>
      <Box position="relative">
        <InputGroup size="sm" width="240px">
          <InputLeftElement pointerEvents="none">
            <SearchIcon color={colors.foreground.secondary} />
          </InputLeftElement>
          <Input
            {...getInputProps({
              id: "ontology-table-picker",
              // Typing replaces the chosen name rather than extending it.
              onFocus: (event) => {
                // Selecting during focus loses to the caret the focusing
                // click places, so select once the click is done.
                const input = event.currentTarget;
                setTimeout(() => input.select());
              },
              onKeyDown: (event) => {
                if (
                  event.key === "Enter" &&
                  highlightedIndex === -1 &&
                  inputValue.trim() !== ""
                ) {
                  onChange(inputValue.trim());
                }
              },
            })}
            placeholder="Find a table"
            fontFamily="mono"
            pl="32px"
          />
        </InputGroup>
        <List
          {...getMenuProps()}
          position="absolute"
          top="100%"
          left="0"
          mt="1"
          minWidth="320px"
          bg={colors.background.primary}
          border={showMenu ? "1px solid" : "none"}
          borderColor={colors.border.secondary}
          borderRadius="md"
          boxShadow={showMenu ? "md" : "none"}
          maxHeight="280px"
          overflowY="auto"
          zIndex={10}
          py={showMenu ? "1" : "0"}
        >
          {showMenu &&
            candidates.map((candidate, index) => (
              <ListItem
                key={candidate.relation.id}
                {...getItemProps({ item: candidate, index })}
                px="3"
                py="1.5"
                cursor="pointer"
                background={
                  highlightedIndex === index
                    ? colors.background.secondary
                    : undefined
                }
              >
                <HStack justify="space-between" spacing={3}>
                  <Text textStyle="text-small" fontFamily="mono">
                    {candidate.name}
                  </Text>
                  {candidate.hasShape && (
                    <Badge variant="subtle" textTransform="none" fontSize="xs">
                      ontology columns
                    </Badge>
                  )}
                </HStack>
              </ListItem>
            ))}
        </List>
      </Box>
    </HStack>
  );
};
