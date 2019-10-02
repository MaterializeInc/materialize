#Copyright 2014 Florian Wolf, SAP AG
#
#Modifications copyright 2019 Materialize, Inc.
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.

CXXFLAGS = -std=c++17 -Wall -Wextra -Werror
ifeq ($(DEBUG),1)
CXXFLAGS += -g -O0
else
CXXFLAGS += -O2
endif

LDLIBS = -lodbc -lpthread

SRCDIR = src
BUILDDIR = build

SOURCES = \
	$(SRCDIR)/AnalyticalStatistic.cc \
	$(SRCDIR)/chBenchmark.cc \
	$(SRCDIR)/DataSource.cc \
	$(SRCDIR)/DbcTools.cc \
	$(SRCDIR)/dialect/DialectStrategy.cc \
	$(SRCDIR)/Log.cc \
	$(SRCDIR)/PthreadShim.cc \
	$(SRCDIR)/Queries.cc \
	$(SRCDIR)/Random.cc \
	$(SRCDIR)/Schema.cc \
	$(SRCDIR)/TransactionalStatistic.cc \
	$(SRCDIR)/Transactions.cc \
	$(SRCDIR)/TupleGen.cc

$(BUILDDIR)/%.o : $(SRCDIR)/%.cc
	mkdir -p $(@D) && $(COMPILE.cc) $< -o $@

OBJS = $(subst $(SRCDIR),$(BUILDDIR),$(SOURCES:.cc=.o))

LINK.o = $(LINK.cc)

$(BUILDDIR)/chBenchmark: $(OBJS)

clean:
	rm -rf $(BUILDDIR)
