#!/bin/sh

# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
#
# install-tools.sh — installs the Hugo and Dart Sass builds pinned in
# ci/builder/Dockerfile, so the deploy builds with the same tools as lint-docs
# without running the ci-builder image.

set -eu

dockerfile=ci/builder/Dockerfile

pin() {
    sed -n "s/^ARG $1=//p" "$dockerfile"
}

fetch() {
    curl -fsSL "$1" > "$2"
    echo "$3  $2" | sha256sum --check
}

hugo=$(pin HUGO_AMD64)
hugo_version=${hugo%@*}
fetch "https://github.com/gohugoio/hugo/releases/download/$hugo_version/hugo_withdeploy_${hugo_version#v}_linux-amd64.tar.gz" hugo.tar.gz "${hugo#*@}"
sudo tar -xzf hugo.tar.gz -C /usr/local/bin hugo
rm hugo.tar.gz

sass=$(pin DART_SASS_AMD64)
sass_version=${sass%@*}
fetch "https://github.com/sass/dart-sass/releases/download/$sass_version/dart-sass-$sass_version-linux-x64.tar.gz" dart-sass.tar.gz "${sass#*@}"
sudo tar -xzf dart-sass.tar.gz -C /opt
sudo ln -sf /opt/dart-sass/sass /usr/local/bin/sass
rm dart-sass.tar.gz
