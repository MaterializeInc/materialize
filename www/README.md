This directory contains the public Materialize website.

# Overview

`materialize/www` is meant to be used as a [Hugo](https://gohugo.io) website.

To view the website:

1. [Install Hugo](https://gohugo.io/getting-started/installing/).
1. Launch the site by running:

    ```shell
    cd <path to this dir>
    hugo server -D
    ```
1. View the site by going to <http://localhost:1313>, or go to the docs at
   <http://localhost:1313/docs>.

You can also read the documentation in Markdown in the `/doc/user` directory,
though some features like our SQL syntax diagrams will not be readily
accessible.

# Structure

- `archetypes`: Metadata templates for new docs.
- `assets`: Content used dynamically, i.e. JavaScript and SCSS.
- `content`: All of the docs content, though this is mostly content symlinked to
  `/doc/user`.
- `data`: Any JSON or YAML files you would want to use as a datastore.
- `layouts`: All of the HTML templates for the site.
    - `partials`: Many HTML components, as well as our SQL diagrams.
- `resources`: The results of dynamically generating content from `assets`.
- `static`: Static files for the site, such as images and fonts.
- `util`: Materialize-developed utilities for the site, e.g. the documentation's
  railroad diagram generator.

# Tasks

## Updating CSS

No CSS is shared with the marketing website to keep the docs CSS maintainable.
If the marketing website changes, we will need to update the docs CSS to
visually match.

### General stylesheet updates

You can see how commonly rendered elements look by going to
[`localhost:1313/stylesheet`](http://localhost:1313/stylesheet).

You can use this as a scratch area by editing `/www/content/stylesheet.md`.

### Syntax highlighting

We use Hugo's built-in support for syntax highlighting through Chroma. In
`config.toml`:

```toml
pygmentsCodeFences = true
pygmentsStyle = "xcode"
```

This will probably need to be changed at some point in the future to allow for
highlighting Materialized extensions to the SQL standard, as well as generally
beautifying the syntax highlighting color scheme––but for right now, what's
there suffices.

You can adjust the highlight colors as necessary in
`www/assets/_highlight.scss`.

# Known limitations

- Cannot display formatted text in descriptions or menus (e.g. cannot format
  page titles in code blocks).
- Does not support more than 2 levels in menus.
- Is not "responsive" and makes naive decisions about breakpoints. If someone
  would like to volunteer their web development expertise to make this more
  sane, I would be really happy to help them out.

# Miscellany, Trivia, & Footguns

- Headers are automatically hyperlinked using [AnchorJS].
- Railroad diagrams are managed in this directory at
  `/www/layouts/partials/sql-grammar` but are more properly part of the docs
  site. Get more details in </doc/user/README.md>.

[AnchorJS]: https://www.bryanbraun.com/anchorjs/
