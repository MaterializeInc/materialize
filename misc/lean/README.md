# Lean models

This directory is a Lake project for Lean 4 models of Materialize.
Each model lives under `Mz/` and is imported from `Mz.lean`, so `lake build` checks all of them.
The `lean` CI step runs that build, and `warningAsError` in `lakefile.toml` makes any `sorry` or warning fail it.
`Mz/Collection.lean` is a sample: it models collections of `(record, diff)` updates and proves that consolidation preserves multiplicities.

## Running

Check all models in Docker, the same way CI does:

```sh
bin/mzcompose --find lean run default
```

Arguments replace the default `build` and go to `lake`, for example `bin/mzcompose --find lean run default env lean Mz/Collection.lean`.
Separate arguments that start with `-` from the workflow's own options with `--`.

For editor support, install [elan](https://github.com/leanprover/elan) and open this directory.
elan picks the Lean version from `lean-toolchain`.

## Toolchain and dependencies

The `lean` mzbuild image in `misc/images/lean` installs the toolchain named in `lean-toolchain` and builds the dependencies in `lakefile.toml` and `lake-manifest.json` into the image.
Only those three files are inputs to the image, so editing a model reuses the published image.
To change the Lean version, edit `lean-toolchain`.
To add a dependency, add a `[[require]]` to `lakefile.toml`, pin it by `rev`, and commit the `lake-manifest.json` that `lake update` writes.

## Checking a model

A proof only says something about the system if the model can fail.
Before relying on a model, break it in a way the theorems should catch, for example by removing a guard, and confirm the build fails.
