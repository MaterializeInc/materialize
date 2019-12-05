cmake_minimum_required(VERSION 3.5)
project(chbenchmark)

set(CMAKE_CXX_STANDARD 17)

include_directories(src)
include_directories(src/dialect)

add_executable(chbenchmark
        src/dialect/DialectStrategy.cc
        src/AnalyticalStatistic.cc
        src/chBenchmark.cc
        src/DataSource.cc
        src/DbcTools.cc
        src/Log.cc
        src/PthreadShim.cc
        src/Queries.cc
        src/Random.cc
        src/Schema.cc
        src/TransactionalStatistic.cc
        src/Transactions.cc
        src/TupleGen.cc
        src/materialized.cc
        src/mz-config.cpp src/Histogram.cc src/Histogram.h)

target_link_libraries(chbenchmark
        odbc
        pthread
        pq
        pqxx
        config)
