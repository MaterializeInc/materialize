---
source: src/npm/src/lib.rs
revision: e27cff4813
---

# mz-npm

Downloads and vendors JavaScript/CSS assets from the NPM registry directly (without Node.js), so they can be embedded into the `environmentd` binary at build time.
`NpmPackage` records the package name, version, file paths for production/dev JS and optional CSS, and a SHA-256 digest; the `ensure` function downloads any out-of-date packages from `registry.npmjs.org`, verifies digests, and removes stray vendor files.
Used as a build-time dependency by `mz-environmentd` to populate `src/http/static/` before the `include_dir!` macro bundles the assets into the binary.
