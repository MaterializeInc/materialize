// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { Input } from "@chakra-ui/react";
import React from "react";

import { THRESHOLD_STEP_MS } from "./constants";

export interface ThresholdInputProps {
  valueMs: number;
  onChange: (valueMs: number) => void;
  /** Named for the unit on screen, which is seconds. */
  ariaLabel?: string;
}

/**
 * The threshold in seconds, as a number field.
 *
 * Holds the text separately from the threshold, because the two can disagree:
 * "2." is a reasonable thing to be part-way through typing and not a number.
 * An empty field is the case that matters, since `Number("")` is `0`, and
 * reporting that would mark everything as exceeding mid-keystroke.
 *
 * Only a parseable value is reported. `useThresholdControl` holds it for the
 * field's settle window before anything acts on it, so a field left empty
 * changes nothing and reverts on blur.
 */
export const ThresholdInput = ({
  valueMs,
  onChange,
  ariaLabel = "Threshold in seconds",
}: ThresholdInputProps) => {
  const [text, setText] = React.useState<string | undefined>(undefined);

  return (
    <Input
      type="number"
      size="sm"
      width="20"
      min={0}
      step={THRESHOLD_STEP_MS / 1000}
      aria-label={ariaLabel}
      value={text ?? (valueMs / 1000).toString()}
      onChange={(e) => {
        const raw = e.target.value;
        setText(raw);
        const seconds = Number(raw);
        if (raw.trim() !== "" && Number.isFinite(seconds) && seconds >= 0) {
          onChange(seconds * 1000);
        }
      }}
      onBlur={() => setText(undefined)}
    />
  );
};
