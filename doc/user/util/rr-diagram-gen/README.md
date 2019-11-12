# Automatic Railroad Diagram Generator

Materialize's SQL API documentation relies on a "railroad diagram" to express a statement's syntax. These diagrams are obtained from <https://www.bottlecaps.de/rr/ui>, which is a service that converts EBNF files (used to express a grammar) into SVGs (the aforementioned railroad diagrams). Those SVGs are then appropriate to include in the Materialize docs once they've been tidied up a bit.

## Usage

With Golang installed, run:

```shell
make run
```

which will build the binary in this directory, and convert all BNF files in the appropriate directory into SVGs.
