// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useClipboard } from "@chakra-ui/react";
import React from "react";

/** Wraps Chakras useClipboard hook and adds a `copied` state with a timeout. */
export const useCopyableText = (text: string, delay?: number) => {
  const { onCopy, setValue, hasCopied } = useClipboard(text, delay || 1000);

  React.useEffect(() => {
    setValue(text);
  }, [setValue, text]);

  return {
    onCopy,
    copied: hasCopied,
  };
};
