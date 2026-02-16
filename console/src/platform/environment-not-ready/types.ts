// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

export interface TutorialSlide {
  title: string;
  text: string;
  buttonText: string;
  docsUrl?: string;
  image: {
    src: string;
    alt: string;
  };
}

export type TutorialKey =
  | "creating-environment"
  | "standard-sql"
  | "incremental-updates"
  | "ecosystem-compatibility"
  | "consistency-guarantees";
