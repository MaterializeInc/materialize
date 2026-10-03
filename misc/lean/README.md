# Lean models

This directory is a Lake project for Lean 4 models of Materialize.
Models live in two places: under `Mz/` in this directory, and next to the design doc they belong to, as `doc/developer/design/<dir>/*.lean`.
`lake build` checks every module in both places, whether or not anything imports it.
The `lean` CI step runs that build, and `warningAsError` in `lakefile.lean` makes any `sorry` or warning fail it.
`Mz/Collection.lean` is a sample: it models collections of `(record, diff)` updates and proves that consolidation preserves multiplicities.

## Design doc models

To attach a model to a design doc, put the doc in its own directory under `doc/developer/design/` and add `.lean` files next to it.
`lakefile.lean` lists those directories when Lake reads it, and each one becomes a module namespace.
A directory name that starts with a digit needs quoting, so `doc/developer/design/20261001_topic/Model.lean` is imported as `import «20261001_topic».Model`.
Lake caches what it found, so after adding a directory, run `lake -R build` locally to read `lakefile.lean` again.
The Docker run below always does.

## Running

Check all models in Docker, the same way CI does:

```sh
bin/mzcompose --find lean run default
```

Arguments replace the default `build` and go to `lake`, for example `bin/mzcompose --find lean run default env lean Mz/Collection.lean`.
Only `lake build` applies the options in `lakefile.lean`, so `lake env lean` reports a `sorry` as a warning and succeeds.
Separate arguments that start with `-` from the workflow's own options with `--`.

For editor support, install [elan](https://github.com/leanprover/elan) and open this directory.
elan picks the Lean version from `lean-toolchain`.

## Toolchain and dependencies

The `lean` mzbuild image in `misc/images/lean` installs the toolchain named in `lean-toolchain` and builds the dependencies in `lakefile.lean` and `lake-manifest.json` into the image.
Those three are the only files from this directory that are inputs to the image, so editing a model reuses the published image.
To change the Lean version, edit `lean-toolchain`.
To add a dependency, add a `require` to `lakefile.lean`, pin it to a revision, and commit the `lake-manifest.json` that `lake update` writes.

## Checking a model

A proof only says something about the system if the model can fail.
Before relying on a model, break it in a way the theorems should catch, for example by removing a guard, and confirm the build fails.
