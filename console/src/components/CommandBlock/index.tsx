// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { history } from "@codemirror/commands";
import {
  extensionManager,
  updateListener,
} from "@codemirror-toolkit/extensions";
import React, { PropsWithChildren } from "react";

import Editor, { EditorProps } from "./Editor";
import { EditorCommand, EditorEvent } from "./keymaps";
import languageExt from "./mzDialect";
import { _Provider as Provider } from "./provider";

export const CodeMirrorProvider = ({ children }: PropsWithChildren) => {
  return (
    <Provider
      config={{
        extensions: [
          updateListener(),
          history(),
          languageExt,
          extensionManager(),
        ],
      }}
    >
      {children}
    </Provider>
  );
};

export default Editor;
export type { EditorCommand, EditorEvent, EditorProps };
