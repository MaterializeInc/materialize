# Materialize SQL pretty printer

Pretty print a SQL string.

## Test

After building the package with `bin/wasm-build misc/wasm/src/sql-pretty-wasm`,
run:

```shell
node --experimental-wasm-modules misc/wasm/src/sql-pretty-wasm/test.mjs
```
