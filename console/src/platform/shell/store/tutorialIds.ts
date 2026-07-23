// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export const TUTORIAL_IDS = ["quickstart", "academy"] as const;
export type TutorialId = (typeof TUTORIAL_IDS)[number];

export const TUTORIAL_LABELS: Record<TutorialId, string> = {
  quickstart: "Quickstart",
  academy: "MZ Academy: intro to Materialize",
};

export const TUTORIAL_SHORT_LABELS: Record<TutorialId, string> = {
  quickstart: "Quickstart",
  academy: "MZ Academy",
};
