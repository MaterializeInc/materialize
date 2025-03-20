# Generating new Chroma syntax highlights

Chroma is the syntax highlighter used by Hugo, the static site generator that powers Materialize's docs. We have upstreamed a Materialize lexer (which is a slightly-modified version of their Postgres lexer). When new keywords are added we should upstream an update.

## Generating a new lexer definition

1. Fork the Chroma repo and clone it locally as a sibling of the `materialize` repo.
2. From the root directory of the `materialize` repo, run the generate script:
   ```shell
   ./bin/gen-chroma-syntax
   ```
3. In the Chroma repo, commit the changes to the Materialize dialect file and submit them as a PR.
