// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Box, Flex, Grid, GridItem } from "@chakra-ui/react";
import { useAtomValue, useSetAtom } from "jotai";
import React, { useCallback, useRef } from "react";
import { Outlet, useMatch } from "react-router-dom";

import { resultsPanelOpenAtom, tutorialVisibleAtom } from "./store";
import Tutorial from "./Tutorial";
import { ROW_RETURNING_KINDS, useExecution } from "./useExecution";
import { useSubscribe } from "./useSubscribe";
import type { WorksheetEditorHandle } from "./WorksheetEditor";
import WorksheetEditor from "./WorksheetEditor";
import WorksheetHeader from "./WorksheetHeader";

/** Top-level worksheet page: editor with optional tutorial sidebar. Overlay pages (monitor, workflow, dataflow) render via Outlet. */
const WorksheetPage = () => {
  const isIndex = useMatch("/regions/:regionSlug/shell");
  const hasOverlay = !isIndex;
  const { execute } = useExecution();
  const { start: startSubscribe, reset: resetSubscribe } = useSubscribe();
  const tutorialVisible = useAtomValue(tutorialVisibleAtom);
  const editorRef = useRef<WorksheetEditorHandle>(null);
  const setResultsPanelOpen = useSetAtom(resultsPanelOpenAtom);

  const handleExecute = useCallback(
    (sql: string, kind: string, offset: number) => {
      if (ROW_RETURNING_KINDS.has(kind)) {
        setResultsPanelOpen(true);
      }
      if (kind === "subscribe" || kind === "tail") {
        startSubscribe(sql, offset);
      } else {
        if (ROW_RETURNING_KINDS.has(kind)) {
          resetSubscribe();
        }
        execute(sql, kind, offset);
      }
    },
    [execute, startSubscribe, resetSubscribe, setResultsPanelOpen],
  );

  const handleRunCommand = useCallback(
    (sql: string) => {
      execute(sql, "set");
    },
    [execute],
  );

  return (
    <>
      {hasOverlay && <Outlet />}
      <Flex
        direction="column"
        height="100%"
        overflow="hidden"
        display={hasOverlay ? "none" : "flex"}
      >
        <WorksheetHeader onRunCommand={handleRunCommand} />
        <Grid
          templateAreas={`"main tutorial"`}
          gridTemplateColumns={`minmax(0,1fr) ${tutorialVisible ? "600px" : "0px"}`}
          flex="1"
          overflow="hidden"
        >
          <GridItem area="main" overflow="hidden">
            <Box height="100%" pt="2">
              <WorksheetEditor ref={editorRef} onExecute={handleExecute} />
            </Box>
          </GridItem>
          {tutorialVisible && (
            <Tutorial
              insertAndExecute={(sql) =>
                editorRef.current?.insertAndExecute(sql)
              }
            />
          )}
        </Grid>
      </Flex>
    </>
  );
};

export default WorksheetPage;
