# Copyright 2019 Materialize, Inc. All rights reserved.
#
# This file is part of Materialize. Materialize may not be used or
# distributed without the express permission of Materialize, Inc.

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
BINARY_NAME=rr-diagram-gen
BINARY_UNIX=$(BINARY_NAME)_unix

src_default=../../layouts/partials/sql-grammar/bnf/
dst_default=../../layouts/partials/sql-grammar/rr-diagrams/

all: clean build run

build :
	$(GOBUILD) -o $(BINARY_NAME) -v

clean :
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_UNIX)

run :
	./$(BINARY_NAME) $(src_default) $(dst_default)
