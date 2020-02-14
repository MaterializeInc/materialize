// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Each image has all of its steps in here as a separate item, to make it easy to modify,
// uncomment the step associated with the image you care about and have fun.

import { createGitgraph } from "@gitgraph/js";

const graphContainer = document.getElementById("gitgraph")
const gitgraph = createGitgraph(graphContainer, {author: " ", commitMessage: " "})
const master = gitgraph.branch("master")

// step 1
// master
//     .commit({hash: "A"})
//     .commit({hash: "B"})
//     .commit({hash: "C"})

// // step 2
// master
//     .commit({hash: "A"})
//     .commit({hash: "B"})
//     .tag("v0.1.0-rc1")
//     .commit({hash: "C"})

// // step 3 - release
// master
//     .commit({hash: "A"})
//     .commit({hash: "B"})
//     .tag("v0.1.0-rc1")
//     .tag("v0.1.0")
//     .commit({hash: "C"})


// // step 4 - do work
// master
//     .commit({hash: "A"})
//     .commit({hash: "B"})
//     .tag("v0.1.0-rc1")
//     .commit({hash: "C"})
//     .commit({hash: "D", commitMessage: "fix"})
//     .commit({hash: "E", commitMessage: "fix"})
//     .commit({hash: "F"})

// // step 5 - update
// master
//     .commit({hash: "A"})
//     .commit({hash: "B"})
//     .tag("v0.1.0-rc1")

// const release = gitgraph.branch("release-0.1.0")
// release
//     .commit({hash: "D'"})
//     .commit({hash: "E'"})
//     .tag("v0.1.0-rc2")
// master
//     .commit({hash: "C"})
//     .commit({hash: "D"})
//     .commit({hash: "E"})
//     .commit({hash: "F"})

// step 7 - tag
master
    .commit({hash: "A"})
    .commit({hash: "B"})
    .tag("v0.1.0-rc1")

const release = gitgraph.branch("release-0.1.0")
release
    .commit({hash: "D'"})
    .commit({hash: "E'"})
    .tag("v0.1.0-rc2")
    .tag("v0.1.0")
master
    .commit({hash: "C"})
    .commit({hash: "D"})
    .commit({hash: "E"})
    .commit({hash: "F"})
    .commit({hash: "G"})
    .commit({hash: "H"})
    .tag("v0.1.1-rc1")
