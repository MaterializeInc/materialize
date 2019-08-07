#Copyright 2014 Florian Wolf, SAP AG
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

CXXFLAGS = -c -std=c++17 -O2 -Wall -Wextra -Werror
LDLIBS = -lodbc

SOURCES = \
	src/AnalyticalStatistic.cc \
	src/chBenchmark.cc \
	src/Config.cc \
	src/DataSource.cc \
	src/DbcTools.cc \
	src/dialect/DialectStrategy.cc \
	src/Log.cc \
	src/PthreadShim.cc \
	src/Queries.cc \
	src/Schema.cc \
	src/TransactionalStatistic.cc \
	src/Transactions.cc \
	src/TupleGen.cc

chBenchmark: $(SOURCES:.cc=.o)
	$(CXX) $(LDFLAGS) $(LDLIBS) $^ -o $@

clean:
	find . -name '*.o' -delete
	rm chBenchmark
