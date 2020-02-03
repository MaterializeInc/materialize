cmake_minimum_required(VERSION 3.5)
project(chbenchmark)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}/cmake")

find_package(Threads REQUIRED)
find_package(ODBC REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(Pqxx REQUIRED)
find_package(Config++ REQUIRED)

add_executable(chbenchmark
    src/AnalyticalStatistic.cc
    src/chBenchmark.cc
    src/Config.cc
    src/DataSource.cc
    src/DbcTools.cc
    src/Histogram.cc
    src/Histogram.h
    src/Log.cc
    src/materialized.cc
    src/mz-config.cpp
    src/PthreadShim.cc
    src/Queries.cc
    src/Random.cc
    src/Schema.cc
    src/TransactionalStatistic.cc
    src/Transactions.cc
    src/TupleGen.cc)

target_compile_features(chbenchmark PRIVATE cxx_std_17)

target_include_directories(chbenchmark PRIVATE
    src
    src/dialect
    ${PostgreSQL_INCLUDE_DIRS}
    ${Pqxx_INCLUDE_DIRS}
    ${Config++_INCLUDE_DIRS})

target_link_libraries(chbenchmark
    Threads::Threads
    ODBC::ODBC
    ${PostgreSQL_LIBRARIES}
    ${Pqxx_LIBRARIES}
    ${Config++_LIBRARIES})
