---
title: "Versions"
description: "Materialize's version information"
menu: "main"
weight: 300
---

## Stable releases

Binary tarballs for all stable releases are provided below. Other installation
options are available for the latest stable release on the [Install
page](/install).

{{< version-list >}}

Binary tarballs require a recent version of their stated platform:

* macOS binary tarballs require macOS 10.14 and later.
* Linux binary tarballs require a glibc-based Linux distribution whose kernel
  and glibc versions are compatible with Ubuntu Xenial. Glibc-based Linux
  distributions released mid-2016 or later are likely to be compatible.

## Unstable builds

Binary tarballs are built for every merge to the [master branch on
GitHub][github]. These tarballs are not suitable for use in production.
**Run unstable builds at your own risk.**

Version | Binary tarball links
--------|---------------------
master  | [Linux] / [macOS]

The tarballs for other commits on master can be constructed by replacing
`latest` in the links above with the full 40-character commit hash.

[Linux]: http://downloads.mtrlz.dev/materialized-latest-x86_64-unknown-linux-gnu.tar.gz
[macOS]: http://downloads.mtrlz.dev/materialized-latest-x86_64-apple-darwin.tar.gz
[github]: https://github.com/MaterializeInc/materialize
