# Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

GO = go

SRC = ../../layouts/partials/sql-grammar/bnf
DST = ../../layouts/partials/sql-grammar/rr-diagrams

.PHONY: run
run: rr-diagram-gen
	./rr-diagram-gen $(SRC) $(DST)

rr-diagram-gen: *.go
	$(GO) build -v -o $@ $(GOFLAGS)

.PHONY: clean
clean:
	$(GO) clean $(GOFLAGS)
	rm -f rr-diagram-gen
