// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, BoxProps, useTheme } from "@chakra-ui/react";
import { useAtom } from "jotai";
import debounce from "lodash.debounce";
import React, {
  memo,
  PropsWithChildren,
  useCallback,
  useEffect,
  useMemo,
  useRef,
} from "react";
import AutoSizer from "react-virtualized-auto-sizer";
import {
  areEqual,
  ListChildComponentProps,
  ListOnItemsRenderedProps,
  VariableSizeList,
} from "react-window";

import { useFlags } from "~/hooks/useFlags";
import { MaterializeTheme } from "~/theme";

import { JOTAI_DEBOUNCE_WAIT_MS } from "./constants";
import heightByListItemIndex, { getTotalHeight } from "./heightByListItem";
import HistoryOutput from "./HistoryOutput";
import { historyIdsAtom, shellStateAtom } from "./store/shell";
import {
  ShellVirtualizedListContext,
  useShellVirtualizedList,
} from "./useShellVirtualizedList";

const OVERSCAN_COUNT = 3;
// Arbitrary time found through experimentation.
const TIME_FOR_LIST_ITEM_TO_UPDATE_MS = 200;

function isBottomListItemInDOM(
  visibleStopIndex?: number,
  numListItems?: number,
) {
  if (!visibleStopIndex || !numListItems) return false;
  const lastListIndex = numListItems - 1;
  const overscanStopIndex = visibleStopIndex + OVERSCAN_COUNT;
  /**
   * We subtract 1 from overscanStopIndex since
   * a new list item might've been added to the bottom before React Window could update
   */
  return overscanStopIndex - 1 >= lastListIndex;
}

const ListItem = memo(({ index, style }: ListChildComponentProps) => {
  const [historyIds] = useAtom(historyIdsAtom);
  const listItemRef = useRef<HTMLDivElement | null>(null);
  const { colors } = useTheme<MaterializeTheme>();
  const { getSize, setSize } = useShellVirtualizedList();

  /**
   * Calculates a list item's height and tells the virtualized list to update.
   */
  useEffect(() => {
    if (!listItemRef.current) return;
    const el = listItemRef.current;
    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        if (entry.borderBoxSize) {
          const height = entry.borderBoxSize[0].blockSize;

          const isUnmount = height === 0; // When React umounts a component, it sets its height as 0 first

          if (!isUnmount) {
            setSize(index, height);
          }
        }
      }
    });

    resizeObserver.observe(el);

    return () => {
      resizeObserver.unobserve(el);
    };
  }, [getSize, index, setSize]);

  return (
    <Box
      style={style}
      borderBottomWidth={index === historyIds.length - 1 ? "0px" : "1px"}
      borderBottomColor={colors.border.secondary}
    >
      <HistoryOutput ref={listItemRef} historyId={historyIds[index]} />
    </Box>
  );
}, areEqual);

/**
 * ShellVirtualizedListProvider exposes an API for managing the following DOM related behavior in the Shell:
 * - Scrolling
 * - List virtualization
 */
export const ShellVirtualizedListProvider = ({
  children,
}: PropsWithChildren) => {
  const flags = useFlags();
  const keepAutoscrollOnFeatureFlag = flags["keep-autoscroll-on-1364"];
  const variableSizeListRef = useRef<VariableSizeList>(null);
  const listOuterRef = useRef<HTMLDivElement>(null);

  const shouldAutoScrollRef = useRef<boolean>(false);

  const virtualListRangeInfoRef = useRef<{
    visibleStopIndex: number;
    numListItemsSinceLastUpdate: number;
  } | null>(null);

  const shouldAutoScroll = useCallback(() => {
    /**
     * TODO: Get rid once https://github.com/MaterializeInc/console/issues/1364 is fixed
     *
     * A temporary flag to keep autoscroll on. This is bandaid that fixes the bug since the root cause of it is
     * shouldAutoScroll being set to false when it shouldn't.
     */
    if (keepAutoscrollOnFeatureFlag) {
      return true;
    }
    return shouldAutoScrollRef.current;
  }, [keepAutoscrollOnFeatureFlag]);

  const setShouldAutoScroll = useCallback((val: boolean) => {
    shouldAutoScrollRef.current = val;
  }, []);

  const onItemsRendered = useCallback(
    ({ visibleStopIndex }: ListOnItemsRenderedProps) => {
      virtualListRangeInfoRef.current = {
        visibleStopIndex,
        // heightByListItemIndex.size should consistent by the time onItemsRendered is called
        numListItemsSinceLastUpdate: heightByListItemIndex.size,
      };
    },
    [],
  );

  const getSize = useCallback((index: number) => {
    return heightByListItemIndex.get(index) ?? 0;
  }, []);

  const scrollToBottom = useCallback((scrollBehavior: ScrollBehavior) => {
    /**
     *
     * We use two different scrolling methods as a hack to solve an issue (https://github.com/MaterializeInc/console/issues/1343) where scrollToBottom
     * won't scroll to the bottom when we have a large list item and the user is scrolled up. This is because large list items
     * cause react-window to recalculate and decrease the list's DOM node's scroll height, making .scrollTo scroll partially.
     *
     * The solution to this is to use react-window's built in .scrollToItem method which works as expected. However, this method
     * doesn't support smooth scrolling AND instant scrolling. I've created a request to add this feature here: https://github.com/bvaughn/react-window/issues/749,
     * but in the mean time, we can use .scrollTo to smooth scroll since we always instantly scroll to the bottom when a user runs a command, thus ensuring the list's DOM node's scroll height
     * to be correct by the time we run .scrollTo.
     *
     */
    if (scrollBehavior === "smooth") {
      const domEl = listOuterRef.current;
      if (domEl) {
        domEl.scrollTo({
          top: listOuterRef.current.scrollHeight,
          behavior: "smooth",
        });
      }
    } else {
      variableSizeListRef.current?.scrollToItem(
        heightByListItemIndex.size - 1,
        "end",
      );
    }
  }, []);

  const _requestScrollToBottom = useCallback(() => {
    if (!shouldAutoScroll()) {
      return;
    }

    if (
      isBottomListItemInDOM(
        virtualListRangeInfoRef.current?.visibleStopIndex,
        virtualListRangeInfoRef.current?.numListItemsSinceLastUpdate,
      )
    ) {
      // If the bottom virtual list item is guaranteed to be in the DOM, we can smoothly scroll to the bottom.
      scrollToBottom("smooth");
    } else {
      // We initially scroll to the bottom since it's possible the last item in the list isn't rendered.
      // We need the last item to be rendered since otherwise, our Virtualized list {
      // can't calculate its height and perform a scroll properly.
      scrollToBottom("instant");
      setTimeout(() => {
        // There's a bit of a delay to calculate the height. Thus we use a setTimeout to ensure we end at the bottom.
        scrollToBottom("instant");
      }, TIME_FOR_LIST_ITEM_TO_UPDATE_MS);
    }
  }, [scrollToBottom, shouldAutoScroll]);

  const requestScrollToBottom = useMemo(
    () =>
      // Debounce to batch all requests within a small time period into a single scroll
      // This should be valid, because _requestScrollToBottom isn't executed during render,
      // just refererenced.
      // https://github.com/facebook/react/issues/31290
      // eslint-disable-next-line react-compiler/react-compiler
      debounce(_requestScrollToBottom, JOTAI_DEBOUNCE_WAIT_MS),
    [_requestScrollToBottom],
  );

  const isScrolledToBottom = useCallback(() => {
    const domEl = listOuterRef.current;
    return (
      domEl !== null &&
      domEl?.scrollTop + domEl?.clientHeight >= domEl?.scrollHeight
    );
  }, []);

  const setSize = useCallback((index: number, newSize: number) => {
    const oldSize = heightByListItemIndex.get(index);

    if (oldSize !== newSize) {
      heightByListItemIndex.set(index, newSize);
      variableSizeListRef.current?.resetAfterIndex(index);
    }
  }, []);

  return (
    <ShellVirtualizedListContext.Provider
      value={{
        getSize,
        scrollToBottom: requestScrollToBottom,
        isScrolledToBottom,
        onItemsRendered,
        setSize,
        variableSizeListRef,
        listOuterRef,
        shouldAutoScroll,
        setShouldAutoScroll,
      }}
    >
      {children}
    </ShellVirtualizedListContext.Provider>
  );
};

const ShellVirtualizedList = (props: BoxProps) => {
  const [historyIds] = useAtom(historyIdsAtom);
  const {
    variableSizeListRef,
    listOuterRef,
    getSize,
    isScrolledToBottom,
    shouldAutoScroll,
    setShouldAutoScroll,
    onItemsRendered,
  } = useShellVirtualizedList();
  const [shellState] = useAtom(shellStateAtom);

  return (
    <Box {...props}>
      <AutoSizer>
        {({ height, width }) => {
          return (
            <VariableSizeList
              height={height}
              width={width}
              itemCount={historyIds.length}
              itemSize={getSize}
              ref={variableSizeListRef}
              outerRef={listOuterRef}
              className={
                "shell-container " +
                (shellState.crtEnabled ? "crt-enabled" : "")
              }
              onItemsRendered={onItemsRendered}
              onScroll={({ scrollDirection }) => {
                if (
                  scrollDirection === "backward" &&
                  !isScrolledToBottom() &&
                  shouldAutoScroll()
                ) {
                  setShouldAutoScroll(false);
                } else if (isScrolledToBottom() && !shouldAutoScroll()) {
                  // Decide to auto scroll if a user scrolls all the way down
                  setShouldAutoScroll(true);
                }
              }}
              initialScrollOffset={getTotalHeight(heightByListItemIndex)}
              overscanCount={OVERSCAN_COUNT}
            >
              {ListItem}
            </VariableSizeList>
          );
        }}
      </AutoSizer>
    </Box>
  );
};

export default ShellVirtualizedList;
