# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
