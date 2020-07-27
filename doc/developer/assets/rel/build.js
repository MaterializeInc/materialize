#!/usr/bin/env node

// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

const fs = require("fs").promises;
const puppeteer = require("puppeteer");
const Svgo = require("svgo");

const svgo = new Svgo({
    js2svg: {pretty: true},
});

const html = `<!doctype html>
<html>
    <head></head>
    <body>
        <div id="gitgraph"></div>
    </body>
</html>`;

const graphs = [
    {
        name: "01-clean",
        build: () => {
            const main = gitgraph.branch("main");
            main
                .commit({hash: "A"})
                .commit({hash: "B"})
                .commit({hash: "C"});
        },
    },
    {
        name: "02-tagged",
        build: () => {
            const main = gitgraph.branch("main");
            main
                .commit({hash: "A"})
                .commit({hash: "B"})

            const rel = main
                .branch("release-0.1.0");
            rel
                .commit({hash: "D"})
                .tag("v0.1.0-rc1");
            main.commit({hash: "C"});
        },
    },
    {
        name: "02.1-merged",
        build: () => {
            const main = gitgraph.branch("main");
            main
                .commit({hash: "A"})
                .commit({hash: "B"})

            const rel = main
                .branch("release-0.1.0");
            rel
                .commit({hash: "D"})
                .tag("v0.1.0-rc1");
            main.commit({hash: "C"});
            const prep = rel.branch("prepare-v0.1.1");
            prep.commit({hash: "E"})
            main.merge(prep)
        },
    },
    {
        name: "03-tagged-release",
        build: () => {
            const main = gitgraph.branch("main");
            main
                .commit({hash: "A"})
                .commit({hash: "B"})

            const rel = main
                .branch("release-0.1.0");
            rel
                .commit({hash: "D"})
                .tag("v0.1.0-rc1")
            main.commit({hash: "C"});
            const prep = rel.branch("prepare-v0.1.1");

            rel
                .commit({hash: "F"})
                .tag("v0.1.0");

            prep.commit({hash: "E"});
            main.merge(prep);
        }
    },
    {
        name: "04-main-progressed",
        build: () => {
            const main = gitgraph.branch("main");
            main
                .commit({hash: "A"})
                .commit({hash: "B"})
                .tag("v0.1.0-rc1")
                .commit({hash: "C"})
                .commit({hash: "D", commitMessage: "fix"})
                .commit({hash: "E", commitMessage: "fix"})
                .commit({hash: "F"});
        }
    },
    {
        name: "05-tagged-rc2",
        build: () => {
            const main = gitgraph.branch("main");
            main
                .commit({hash: "A"})
                .commit({hash: "B"})
                .tag("v0.1.0-rc1")
            const release = gitgraph.branch("release-0.1.0")
            release
                .commit({hash: "D'"})
                .commit({hash: "E'"})
                .tag("v0.1.0-rc2")
            main
                .commit({hash: "C"})
                .commit({hash: "D"})
                .commit({hash: "E"})
                .commit({hash: "F"});
        }
    },
    {
        name: "06-release-010",
        build: () => {
            const main = gitgraph.branch("main");
            main
                .commit({hash: "A"})
                .commit({hash: "B"})
                .tag("v0.1.0-rc1")
            const release = gitgraph.branch("release-0.1.0")
            release
                .commit({hash: "D'"})
                .commit({hash: "E'"})
                .tag("v0.1.0-rc2")
                .tag("v0.1.0")
            main
                .commit({hash: "C"})
                .commit({hash: "D"})
                .commit({hash: "E"})
                .commit({hash: "F"});
        }
    },
    {
        name: "07-release-011",
        build: () => {
            const main = gitgraph.branch("main");
            main
                .commit({hash: "A"})
                .commit({hash: "B"})
                .tag("v0.1.0-rc1")
            const release = gitgraph.branch("release-0.1.0")
            release
                .commit({hash: "D'"})
                .commit({hash: "E'"})
                .tag("v0.1.0-rc2")
                .tag("v0.1.0")
            main
                .commit({hash: "C"})
                .commit({hash: "D"})
                .commit({hash: "E"})
                .commit({hash: "F"})
                .commit({hash: "G"})
                .commit({hash: "H"})
                .tag("v0.1.1-rc1");
        }
    }
];

(async () => {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    await page.setContent(html);

    await page.addScriptTag({
        path: "./node_modules/@gitgraph/js/lib/gitgraph.umd.js",
    });

    for (const graph of graphs) {
        await page.evaluate(() => {
            const graphContainer = document.querySelector("#gitgraph");
            graphContainer.innerHTML = "";
            window.gitgraph = GitgraphJS.createGitgraph(graphContainer, {
                author: " ",
                commitMessage: " "
            });
            const svg = document.querySelector("#gitgraph svg");
            svg.setAttribute("xmlns", "http://www.w3.org/2000/svg");
            svg.setAttribute("xmlns:xlink", "http://www.w3.org/1999/xlink");
        });
        await page.evaluate(graph.build);
        // Building the graph is racy, and it's unclear why. Just wait for the
        // SVG to have contents before scraping it.
        await page.waitForSelector("#gitgraph svg *");
        const svg = await page.$eval("#gitgraph", e => e.innerHTML);
        await fs.writeFile(`${graph.name}.svg`, (await svgo.optimize(svg)).data);
    }

    await browser.close();
  })();
