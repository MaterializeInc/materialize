// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { atom } from "jotai";

import { appConfig } from "./AppConfig";

// Global atom storing the application configuration computed at initialization.
// Using an atom provides better testability through mocking and improved
// developer tooling support. Should never be updated.
export const appConfigAtom = atom(appConfig);
