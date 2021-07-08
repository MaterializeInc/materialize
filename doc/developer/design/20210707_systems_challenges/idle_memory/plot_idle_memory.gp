# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

set terminal svg

set datafile separator ','
set logscale x 2
set logscale y 2
set format y "%.2f"

set ylabel "Rss (GiB)"
set xlabel "Threads"

plot ARG1 using 1:($2/(1024*1024)) with linespoints title "Rss (GiB)"
