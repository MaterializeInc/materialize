# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.
CSV=$(wildcard *.csv)
SVGS=$(patsubst %.csv,%.svg, $(CSV))

%.svg: %.csv plot_idle_stat.gp
	gnuplot -c plot_idle_stat.gp $< > $@

all: $(SVGS)
