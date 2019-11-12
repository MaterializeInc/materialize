This directory contains the public Materialize website.

# Overview

`materialize/www` is meant to be used as a [Hugo](https://gohugo.io) website.

To view the website:

1. [Install Hugo](https://gohugo.io/getting-started/installing/).
1. Launch the site by running:

    ```shell
    cd <path to Materialize docs, a.k.a this dir>
    hugo server -D
    ```
1. View the site by going to [`localhost:1313/docs`](http://localhost:1313/docs).

You can also read the documentation in Markdown in the `materialize/docs/user`
directory, though some features like our SQL syntax diagrams will not be readily
accessible.

# Structure

- `archetypes`: Metadata templates for new docs.
- `assets`: Content used dynamically, i.e. JavaScript and SCSS.
- `content`: All of the documentation itself, as well as its directory structure.
- `data`: Any JSON files you would want to use as a datastore. Currently unused.
- `layouts`: All of the HTML templates for the site.
    - `partials`: Many HTML components, as well as our SQL diagrams.
- `public`: The generated site.
- `resources`: The results of dynamically generating content from `assets`.
- `static`: Static files for the site, such as images and fonts.
- `themes`: Unused; everything is handrolled in layouts.

# Tasks

## Updating CSS

To allow the docs to be kept in concert with the FOS, all of the docs SASS files are prepended with `_docs`. This way, all one ever needs to do is drag over all of the FOS's SASS files, and things should be straightforward to synchronize.

To make updates to the docs' CSS, feel free to create new files as needed, ensuring they continue being prepended with `_docs`.

### General stylesheet updates

You can see how commonly rendered elements look by going to [`localhost:1313/stylesheet`](http://localhost:1313/stylesheet).

You can use this as a scratch area by editing `materialize/www/content/stylesheet.md`.

### Syntax highlighting

We use Hugo's built-in support for syntax highlighting through Rouge. `config.toml`

```toml
pygmentsCodeFences = true
pygmentsStyle = "xcode"
```

This will probably need to be changed at some point in the future to allow for highlighting Materialized extensions to the SQL standard, as well as generally beautifying the syntax highlighting color scheme––but for right now, what's there suffices.

However, you can add any hacks you need to `www/assets/sass/_docs_code.scss`.

## Railroad diagrams

Becuase Materialize doesn't have a defined grammar in its parser, our railroad diagrams are generated from handrolled `.bnf` files. This means that it's possible that our diagrams can fall out-of-sync with the Materialize binaries. C'est la vie.

To generate diagrams:

1. Go to [bottlecaps.de/rr/ui](https://www.bottlecaps.de/rr/ui).
1. Go to the **Edit Grammar** tab.
1. Enter the BNF content you want to generate a diagram for.
1. Go to **Options**.
1. Set **Graphics width** to 600px.
1. Go to **View Diagram**.
1. Click **Download**.
1. Find the `<svg>` tags, remove any `<def>` and `<style>` tags with in it, and then, copy the `<svg>` into a file in the `docs/layouts/partials/sql-diagrams/rr-diagrams` dir as an `html` file. You should also save the BNF you used in `docs/layouts/partials/sql-diagrams/bnf` as a `.bnf` file's.
1. Add the following tag to the page where you want to add the diagram:

    ```shell
    {{< diagram "filename.html" >}}
    ```

Note that this process will be more automated in the near future.

# Known limitations

- Cannot display formatted text in descriptions or menus (e.g. cannot format page titles in code blocks).
- Does not support more than 2 levels in menus.
- Headers are not linkable.
- Pages have no TOC feature.
- Is not "responsive" and makes naive decisions about breakpoints. If someone would like to volunteer their web development expertise to make this more sane, I would be really happy to help them out.

# Miscellany, Trivia, & Footguns

- Headers are automatically hyperlinked using `/doc/user/layouts/partials/content-parser.html`, inspired by [this Hugo thread](https://discourse.gohugo.io/t/adding-anchor-next-to-headers/1726/8), and more specifically [kaushalmodi/hugo-onyx-theme](https://github.com/kaushalmodi/hugo-onyx-theme/blob/cd232177f1af37f5371d252f8401ce049dc52db8/layouts/partials/headline-hash.html).
