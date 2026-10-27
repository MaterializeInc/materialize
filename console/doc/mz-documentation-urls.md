# Handling Materialize Documentation URLs

When referencing Materialize documentation URLs, we derive from a single JSON `mz-doc-urls.json` that gets updated by running the following yarn script: `yarn run gen:mz-doc-urls`.

## How it works

We use `https://materialize.com/docs/sitemap.xml` as the source of truth, initiate a fetch request, then convert it into a flat JSON object.
