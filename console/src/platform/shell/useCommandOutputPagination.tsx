// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtom } from "jotai";
import { useAtomCallback } from "jotai/utils";
import { useCallback, useEffect } from "react";

import {
  HistoryId,
  historyItemAtom,
  updateDisplayStateAtomCallback,
} from "./store/shell";

function calculateTotalPages(numRows: number, pageSize: number) {
  return numRows === 0 ? 1 : Math.ceil(numRows / pageSize);
}

/**
 * Given the current page (zero-indexed), total number of rows, and rows per page, calculate the start and end indexes of the current page.
 *
 * i.e. If there are 20 rows, 10 rows per page, and the current page is 0, the pagination range is [0-10]
 */
function calculateCurrentPageIndexes(
  currentPage: number,
  numRows: number,
  pageSize: number,
) {
  const startIndex = Math.min(numRows, pageSize * currentPage);
  const nextPage = currentPage + 1;
  const endIndex = Math.min(numRows, pageSize * nextPage);

  return [startIndex, endIndex];
}

const usePaginationParameters = (
  historyId: HistoryId,
  commandIndex: number,
  totalPages: number,
) => {
  const [historyItem] = useAtom(historyItemAtom(historyId));
  if (!historyItem || historyItem.kind !== "command") {
    return undefined;
  }
  const displayState = historyItem.commandResultsDisplayStates[commandIndex];

  const { isSubscribeManager, isFollowingSubscribeManager } = displayState;
  const shouldFollowLastPage =
    isSubscribeManager && isFollowingSubscribeManager;

  const currentPage = isSubscribeManager
    ? displayState.currentSubscribeManagerTablePage
    : displayState.currentTablePage;

  const isFirstPage = currentPage === 0;

  const isLastPage = currentPage === totalPages - 1;

  return {
    isSubscribeManager,
    isFollowingSubscribeManager,
    shouldFollowLastPage,
    currentPage,
    isFirstPage,
    isLastPage,
  };
};

export const useCommandOutputPagination = ({
  historyId,
  commandIndex,
  pageSize,
  totalNumRows,
}: {
  historyId: HistoryId;
  commandIndex: number;
  pageSize: number;
  totalNumRows: number;
}) => {
  const updateDisplayState = useAtomCallback(updateDisplayStateAtomCallback);
  const totalPages = calculateTotalPages(totalNumRows, pageSize);
  const paginationParameters = usePaginationParameters(
    historyId,
    commandIndex,
    totalPages,
  );

  const changePage = useCallback(
    (newPage: number, options?: { isManuallyInvoked: boolean }) => {
      updateDisplayState(historyId, commandIndex, (curDisplayState) => {
        const { isSubscribeManager, isFollowingSubscribeManager } =
          curDisplayState;

        const shouldStopFollowing = !!(
          options?.isManuallyInvoked && isSubscribeManager
        );

        return {
          ...curDisplayState,
          [isSubscribeManager
            ? "currentSubscribeManagerTablePage"
            : "currentTablePage"]: newPage,
          isFollowingSubscribeManager: shouldStopFollowing
            ? false
            : isFollowingSubscribeManager,
        };
      });
    },
    [commandIndex, historyId, updateDisplayState],
  );

  useEffect(() => {
    if (
      paginationParameters &&
      paginationParameters.shouldFollowLastPage &&
      !paginationParameters.isLastPage
    ) {
      changePage(totalPages - 1);
    }
  }, [changePage, totalNumRows, totalPages, paginationParameters]);

  if (paginationParameters === undefined) {
    return undefined;
  }

  const {
    currentPage,
    isLastPage,
    isFirstPage,
    isSubscribeManager,
    isFollowingSubscribeManager,
  } = paginationParameters;

  const [startIndex, endIndex] = calculateCurrentPageIndexes(
    currentPage,
    totalNumRows,
    pageSize,
  );

  const onNextPage = () => {
    if (isLastPage) {
      return;
    }
    const nextPage = currentPage + 1;

    changePage(nextPage, { isManuallyInvoked: true });
  };

  const onPrevPage = () => {
    if (isFirstPage) {
      return;
    }

    const prevPage = currentPage - 1;
    changePage(prevPage, { isManuallyInvoked: true });
  };

  const onToggleFollow = () => {
    updateDisplayState(historyId, commandIndex, (curDisplayState) => {
      const { isFollowingSubscribeManager: currentFollowing } = curDisplayState;
      return {
        ...curDisplayState,
        isFollowingSubscribeManager: !currentFollowing,
      };
    });
  };

  // Change the current page if a SUBSCRIBE table decreases its total number of pages
  if (currentPage > totalPages - 1) {
    changePage(totalPages - 1);
  }

  return {
    startIndex,
    endIndex,
    currentPage,
    onNextPage,
    onPrevPage,
    prevEnabled: !isFirstPage,
    nextEnabled: !isLastPage,
    isFollowing: isSubscribeManager ? isFollowingSubscribeManager : null,
    onToggleFollow,
    totalPages,
  };
};

export default useCommandOutputPagination;
