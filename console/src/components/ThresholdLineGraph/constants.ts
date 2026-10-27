// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * How long a dragged threshold line must sit still before the page follows it.
 *
 * The handle has to track the cursor every frame, but the work behind it does
 * not. A consumer typically writes a search param and rebuilds a row per
 * object, and either at pointer-move rate is what makes dragging stutter.
 * Short enough that a reader who stops moving sees the page catch up as one
 * motion rather than as a delay.
 */
export const THRESHOLD_DRAG_SETTLE_MS = 120;

/**
 * How long the threshold field waits after a keystroke.
 *
 * Longer than the drag window. A pointer emits continuously, so 120ms of
 * stillness means the reader has stopped. Digits arrive with gaps that long
 * between them, so the same window would commit "2" on the way to "25".
 */
export const THRESHOLD_INPUT_SETTLE_MS = 400;

/** Smallest change a drag, a key or the field can make, in milliseconds. */
export const THRESHOLD_STEP_MS = 100;
