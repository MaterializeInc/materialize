// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import { writeFileSync } from "node:fs";

import { XMLParser } from "fast-xml-parser";
import retry from "p-retry";

/**
 * Function to fetch sitemap XML and process it into a JSON file
 *
 * @param xmlFetchUrl The URL of the sitemap XML file to fetch.
 * @param jsonWritePath The file path where the generated JSON file will be written.
 * @param urlOrigin The origin URL to prepend to each URL in the sitemap.
 */
async function generateSitemapJSON(
  xmlFetchUrl = "https://materialize.com/docs/sitemap.xml",
  jsonWritePath = "src/mz-doc-urls.json",
  urlOrigin = "https://materialize.com",
) {
  try {
    const res = await retry(
      async () => {
        const _res = await fetch(xmlFetchUrl);
        if (!_res.ok) {
          throw new Error(`Failed to fetch sitemap: ${_res.statusText}`);
        }
        return _res;
      },
      { retries: 4 },
    );

    const xmlData = await res.text();
    const parser = new XMLParser();

    const parsedXMLData = parser.parse(xmlData);

    if (
      !parsedXMLData ||
      !parsedXMLData.urlset ||
      !Array.isArray(parsedXMLData.urlset.url)
    ) {
      throw new Error(`Sitemap has incorrect formatting: ${parsedXMLData}`);
    }

    const urls = (parsedXMLData.urlset.url as any[]).reduce(
      (accum, urlObject) => {
        if (!urlObject || !urlObject.loc || typeof urlObject.loc !== "string") {
          throw new Error(
            `Sitemap has incorrect formatting: URL is missing loc: ${urlObject}`,
          );
        }

        accum[urlObject.loc] = `${urlOrigin}${urlObject.loc}`;

        return accum;
      },
      {},
    ) as { [key: string]: string };

    const jsonOutput = JSON.stringify(urls, null, 2);

    writeFileSync(jsonWritePath, jsonOutput);
    console.info(`Sitemap written to ${jsonWritePath}`);
  } catch (error) {
    console.error("Error processing sitemap:", error);
  }
}

generateSitemapJSON(process.argv[2], process.argv[3], process.argv[4]);
