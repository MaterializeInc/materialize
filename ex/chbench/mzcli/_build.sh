#!/usr/bin/env bash

# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

set -eo pipefail

set -x

zippackage=mzcli-mtrlz.zip
mzcli_backupdir="$HOME/Downloads/mzcli-backups"

for fname in "${HOME}"/Downloads/mzcli-mtrlz* ; do
    if [[ -f $fname ]] ; then
        mkdir -p "$mzcli_backupdir"
        mv "$fname" "$mzcli_backupdir"
    fi
done

echo -n "Your web browser will open, please approve the download and then press enter to continue"
open https://github.com/MaterializeInc/mzcli/archive/mtrlz.zip
read -r

mv "${HOME}"/Downloads/mzcli-mtrlz* .


if [[ ! -f $zippackage ]] ; then
    zip -r $zippackage mzcli-mtrlz
fi

docker build -t materialize/mzcli .
docker tag materialize/mzcli materialize/mzcli:"$(date +%Y-%m-%d)"
