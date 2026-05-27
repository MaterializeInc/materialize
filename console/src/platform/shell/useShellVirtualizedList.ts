// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { createContext, RefObject, useContext } from "react";
import { ListOnItemsRenderedProps, VariableSizeList } from "react-window";

export type ShellVirtualizedListContextData = {
  getSize: (index: number) => number;
  scrollToBottom: () => void;
  isScrolledToBottom: () => boolean;
  setSize: (index: number, newSize: number) => void;
  variableSizeListRef: RefObject<VariableSizeList>;
  listOuterRef: RefObject<HTMLDivElement>;

  /**
   * If shouldAutoScroll returns false, we don't automatically scroll down when someone runs a command function.
   * This is useful when a person is actively looking at old command outputs and SUBSCRIBE is populating a table.
   */
  shouldAutoScroll: () => boolean;
  setShouldAutoScroll: (val: boolean) => void;

  onItemsRendered: (listRangeInfo: ListOnItemsRenderedProps) => void;
};

export const ShellVirtualizedListContext = createContext<
  ShellVirtualizedListContextData | undefined
>(undefined);

export const useShellVirtualizedList = () => {
  const context = useContext(ShellVirtualizedListContext);
  if (!context) {
    throw new Error(
      "useShellVirtualizedListContext must be used within a ShellVirtualizedListProvider",
    );
  }
  return context;
};
