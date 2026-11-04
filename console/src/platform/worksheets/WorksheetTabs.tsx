// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import {
  Button,
  HStack,
  IconButton,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
  Portal,
  Text,
  useTheme,
} from "@chakra-ui/react";
import { useAtom, useStore } from "jotai";
import React, { useState } from "react";
import { v4 as uuidv4 } from "uuid";

import ConfirmationDialog from "~/components/ConfirmationDialog";
import { CloseIcon } from "~/icons";
import { MaterializeTheme } from "~/theme";

import {
  DEFAULT_TAB_ID,
  EMPTY_WORKSHEET,
  Worksheet,
  worksheetAtomFamily,
  worksheetStorageId,
  worksheetStorageKey,
  WorksheetTab,
  worksheetTabsAtomFamily,
} from "./store";

export interface WorksheetTabsProps {
  regionId: string;
  /** Whether the open worksheet has cells, to confirm before closing it. */
  activeHasCells: boolean;
}

/** The strip of open worksheets, with a menu to open new ones. */
export const WorksheetTabs = ({
  regionId,
  activeHasCells,
}: WorksheetTabsProps) => {
  const { colors } = useTheme<MaterializeTheme>();
  const store = useStore();
  const [{ tabs, activeId }, setTabs] = useAtom(
    worksheetTabsAtomFamily(regionId),
  );
  const [closing, setClosing] = useState<WorksheetTab>();

  const open = (title: string, worksheet: Worksheet) => {
    const id = uuidv4();
    store.set(worksheetAtomFamily(worksheetStorageId(regionId, id)), worksheet);
    setTabs((prev) => ({
      activeId: id,
      tabs: [...prev.tabs, { id, title }],
    }));
  };

  const close = (tab: WorksheetTab) => {
    const storageId = worksheetStorageId(regionId, tab.id);
    localStorage.removeItem(worksheetStorageKey(storageId));
    worksheetAtomFamily.remove(storageId);
    setTabs((prev) => {
      const index = prev.tabs.findIndex((t) => t.id === tab.id);
      const remaining = prev.tabs.filter((t) => t.id !== tab.id);
      if (remaining.length === 0) {
        // The last tab cannot go, so closing it leaves an empty worksheet.
        return {
          activeId: DEFAULT_TAB_ID,
          tabs: [{ id: DEFAULT_TAB_ID, title: "Worksheet" }],
        };
      }
      const activeIdAfter =
        prev.activeId === tab.id
          ? remaining[Math.min(index, remaining.length - 1)].id
          : prev.activeId;
      return { activeId: activeIdAfter, tabs: remaining };
    });
  };

  const requestClose = (tab: WorksheetTab) => {
    // Only the open worksheet's contents are known without loading the rest,
    // so background tabs always ask.
    if (tab.id === activeId && !activeHasCells) {
      close(tab);
    } else {
      setClosing(tab);
    }
  };

  return (
    <HStack spacing="1" minW="0" overflowX="auto" overflowY="hidden">
      {tabs.map((tab) => {
        const isActive = tab.id === activeId;
        return (
          <HStack
            key={tab.id}
            spacing="0"
            borderRadius="md"
            bg={isActive ? colors.background.tertiary : undefined}
            _hover={{ bg: colors.background.secondary }}
            flexShrink={0}
          >
            <Button
              size="xs"
              variant="inline"
              fontWeight={isActive ? "600" : "400"}
              color={
                isActive
                  ? colors.foreground.primary
                  : colors.foreground.secondary
              }
              aria-current={isActive ? "page" : undefined}
              maxW="56"
              onClick={() => setTabs((prev) => ({ ...prev, activeId: tab.id }))}
            >
              <Text as="span" noOfLines={1}>
                {tab.title}
              </Text>
            </Button>
            {tabs.length > 1 && (
              <IconButton
                aria-label={`Close ${tab.title}`}
                title="Close worksheet"
                icon={<CloseIcon stroke="currentColor" boxSize="2.5" />}
                size="xs"
                variant="inline"
                minW="5"
                onClick={() => requestClose(tab)}
              />
            )}
          </HStack>
        );
      })}
      <Menu placement="bottom-start" isLazy>
        <MenuButton
          as={Button}
          size="xs"
          variant="inline"
          flexShrink={0}
          title="New worksheet"
          aria-label="New worksheet"
        >
          ＋
        </MenuButton>
        <Portal>
          <MenuList fontSize="sm" zIndex="dropdown">
            <MenuItem
              onClick={() =>
                open(`Worksheet ${tabs.length + 1}`, EMPTY_WORKSHEET)
              }
            >
              New worksheet
            </MenuItem>
          </MenuList>
        </Portal>
      </Menu>
      <ConfirmationDialog
        isOpen={Boolean(closing)}
        onClose={() => setClosing(undefined)}
        onConfirm={() => {
          if (closing) close(closing);
          setClosing(undefined);
        }}
        title={`Close ${closing?.title ?? "worksheet"}?`}
        confirmLabel="Close worksheet"
      >
        Its cells will be deleted. Your query history is kept.
      </ConfirmationDialog>
    </HStack>
  );
};
