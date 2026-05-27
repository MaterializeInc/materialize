// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Code, CodeProps, useTheme } from "@chakra-ui/react";
import {
  lineNumbers as codeMirrorLineNumbers,
  placeholder as codeMirrorPlaceholder,
} from "@codemirror/view";
import {
  addExtension,
  addUpdateListener,
  clearExtensions,
} from "@codemirror-toolkit/extensions";
import { useAtomValue } from "jotai";
import React, {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
} from "react";

import { currentPromptValue } from "~/platform/shell/store/prompt";
import { MaterializeTheme } from "~/theme";
import { FocusableElement } from "~/utils/focusableElement";

import { EditorCommand, EditorEvent, keymapHandler } from "./keymaps";
import { useContainerRef, useView, useViewEffect } from "./provider";
import safariCopyPasteHackExtension, {
  enableSafariCopyPasteHackExtension,
} from "./safariHackExt";
import { themeExtensions } from "./theme";

export interface EditorProps {
  autoFocus?: boolean;
  containerProps?: CodeProps;
  placeholder?: string;
  onChange?: (newValue: string) => void;
  onCommand?: (command: EditorCommand) => boolean;
  onKeyDown?: (event: EditorEvent) => boolean;
  lineNumbers?: boolean;
}

function useAdditionalExtensions({
  lineNumbers,
  onCommand,
  onKeyDown,
  placeholder,
}: Pick<
  EditorProps,
  "lineNumbers" | "placeholder" | "onCommand" | "onKeyDown"
>) {
  const { colorMode } = useTheme<MaterializeTheme>();
  const initialized = useRef(false);
  const view = useView();

  const keymapHandlerExt = useCallback(
    () => keymapHandler(onKeyDown, onCommand),
    [onKeyDown, onCommand],
  );
  const placeholderExt = useMemo(
    () => codeMirrorPlaceholder(placeholder ?? ""),
    [placeholder],
  );

  useEffect(() => {
    if (!view || initialized.current) return;
    addExtension(view, themeExtensions[colorMode]);
    addExtension(view, keymapHandlerExt());
    if (lineNumbers) {
      addExtension(view, codeMirrorLineNumbers());
    }
    if (placeholder) {
      addExtension(view, placeholderExt);
    }
    if (enableSafariCopyPasteHackExtension) {
      addExtension(view, safariCopyPasteHackExtension());
    }
    initialized.current = true;
    return () => {
      clearExtensions(view);
      initialized.current = false;
    };
  }, [
    view,
    colorMode,
    keymapHandlerExt,
    lineNumbers,
    placeholder,
    placeholderExt,
  ]);
}

/**
 * When this component remounts – like when we navigate away from the Shell –  we
 * need to re-initialize CodeMirror with the prompt text.
 */
function useInitializePromptOnMount() {
  const initialized = useRef(false);
  const currentView = useView();

  const currentPrompt = useAtomValue(currentPromptValue);

  useEffect(() => {
    if (!currentView || initialized.current) return;
    currentView.dispatch({
      changes: {
        from: 0,
        to: currentView.state.doc.length,
        insert: currentPrompt,
      },
      selection: { anchor: currentPrompt.length },
    });

    initialized.current = true;
  }, [currentView, currentPrompt]);
}

const Editor = forwardRef<FocusableElement, EditorProps>(
  (
    {
      autoFocus = true,
      containerProps,
      onChange,
      onCommand,
      onKeyDown,
      placeholder,
      lineNumbers,
    },
    ref,
  ) => {
    const currentView = useView();
    const containerRef = useContainerRef();
    useViewEffect((view) => {
      return addUpdateListener(view, (update) => {
        if (onChange && update.docChanged) {
          onChange(update.state.doc.toString());
        }
      });
    });
    useInitializePromptOnMount();
    useAdditionalExtensions({ lineNumbers, placeholder, onCommand, onKeyDown });

    // Override the prompt's default focus behavior to focus via CodeMirror's API
    useImperativeHandle(ref, () => ({
      focus(_options?: FocusOptions) {
        if (!currentView) return;
        currentView.focus();
      },
    }));

    useEffect(() => {
      if (!currentView) return;
      if (autoFocus) {
        currentView.focus();
      }
    }, [currentView, autoFocus]);

    return (
      <Code {...(containerProps ?? {})} cursor="text" tabIndex={0}>
        <div ref={containerRef} />
      </Code>
    );
  },
);

export default Editor;
