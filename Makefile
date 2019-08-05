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

CC=g++
CXXFLAGS=-c -std=c++11 -O2 -Wall
LDFLAGS=-lodbc

SOURCES = \
	src/AnalyticalStatistic.cpp \
	src/TransactionalStatistic.cpp \
	src/dialect/DialectStrategy.cpp \
	src/Config.cpp \
	src/Log.cpp \
	src/DbcTools.cpp \
	src/DataSource.cpp \
	src/TupleGen.cpp \
	src/Schema.cpp \
	src/Queries.cpp \
	src/Transactions.cpp \
	chBenchmark.cpp \
	pthread_shim.cpp
OBJECTS=$(SOURCES:.cpp=.o)
EXECUTABLE=chBenchmark

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS)
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@

.cpp.o:
	$(CC) $(CXXFLAGS) $< -o $@

clean:
	rm *.o
	rm src/*.o
	rm src/dialect/*.o
	rm chBenchmark
