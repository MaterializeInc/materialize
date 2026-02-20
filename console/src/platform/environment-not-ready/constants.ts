// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import ConsistencyGuarantees from "~/img/consistency-guarantees.png";
import CreatingEnvironment from "~/img/creating-environment.svg";
import EcosystemCompatibility from "~/img/ecosystem-compatibility.png";
import IncrementalUpdates from "~/img/incremental-updates.svg";
import SQLSupport from "~/img/sql-support.png";
import docUrls from "~/mz-doc-urls.json";

import { TutorialKey, TutorialSlide } from "./types";

export const TUTORIAL_SLIDES: Record<TutorialKey, TutorialSlide> = {
  "creating-environment": {
    title: "We’re creating your environment",
    text: "This might take a few minutes, so we’ve prepared some helpful resources for you to check out while you wait.",
    buttonText: "Get to know Materialize",
    image: {
      src: CreatingEnvironment,
      alt: "Get to know Materialize",
    },
  },
  "incremental-updates": {
    title: "Incremental updates",
    text: "Materialize ensures you stay up-to-date with fast-changing data. It does only what is necessary to keep your data current with incrementally maintained views.",
    buttonText: "The Materialize ecosystem",
    docsUrl: docUrls["/docs/get-started/"] + "#incremental-updates",
    image: {
      src: IncrementalUpdates,
      alt: "Standard SQL support",
    },
  },
  "standard-sql": {
    title: "Standard SQL support",
    text: "Materialize lets you interact with fast-changing data with the same SQL you already know, with full support for complex joins, aggregations, and even recursive SQL.",
    buttonText: "Learn about incremental updates",
    docsUrl: docUrls["/docs/get-started/"] + "#standard-sql-support",
    image: {
      src: SQLSupport,
      alt: "Strong consistency guarantees",
    },
  },
  "consistency-guarantees": {
    title: "Strong consistency guarantees",
    text: "Materialize provides strict serializability, ensuring correct results without delay.",
    buttonText: "Integrate with your data stack",
    docsUrl: docUrls["/docs/get-started/"] + "#strong-consistency-guarantees",
    image: {
      src: ConsistencyGuarantees,
      alt: "Consistency Guarantess",
    },
  },
  "ecosystem-compatibility": {
    title: "Integrate with your data stack ",
    text: "Materialize seamlessly works with upstream sources and other systems in your data stack, including Kafka, PostgreSQL, MySQL, and dbt.",
    buttonText: "Open Console",
    docsUrl: docUrls["/docs/integrations/"],
    image: {
      src: EcosystemCompatibility,
      alt: "PostgreSQL compatibility",
    },
  },
};

export const TUTORIAL_KEYS = Object.keys(TUTORIAL_SLIDES) as TutorialKey[];
