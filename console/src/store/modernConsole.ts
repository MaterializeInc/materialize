// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { useAtomValue, useSetAtom } from "jotai";
import { atomWithStorage } from "jotai/utils";

export const MODERN_CONSOLE_STORAGE_KEY = "mz-modern-console-enabled";

const modernConsoleEnabledAtom = atomWithStorage<boolean>(
  MODERN_CONSOLE_STORAGE_KEY,
  true,
);

export const useModernConsoleEnabled = () =>
  useAtomValue(modernConsoleEnabledAtom);

export const useSetModernConsoleEnabled = () =>
  useSetAtom(modernConsoleEnabledAtom);
