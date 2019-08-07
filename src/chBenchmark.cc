/*
Copyright 2014 Florian Wolf, SAP AG

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#include "AnalyticalStatistic.h"
#include "DataSource.h"
#include "DbcTools.h"
#include "Log.h"
#include "PthreadShim.h"
#include "Queries.h"
#include "Schema.h"
#include "TransactionalStatistic.h"
#include "Transactions.h"
#include "TupleGen.h"
#include "dialect/DialectStrategy.h"

#include <err.h>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <pthread.h>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <unistd.h>

typedef struct {
    pthread_barrier_t* barStart;
    int* runState;
    int threadId;
    SQLHDBC hDBC;
    void* stat;
    int warehouseCount;
} threadParameters;

static void* analyticalThread(void* args) {

    threadParameters* prm = (threadParameters*)args;
    AnalyticalStatistic* aStat = (AnalyticalStatistic*)prm->stat;

    bool b;
    long query = 0;
    int q = 0;

    Queries* queries = new Queries();
    if (!queries->prepareStatements(prm->hDBC)) {
        exit(1);
    }

    pthread_barrier_wait(prm->barStart);

    if (*(prm->runState) == 1) {
        Log::l1() << Log::tm() << "-analytical " << prm->threadId
                  << ":  start warmup\n";
        while (*(prm->runState) == 1) {
            q = (query % 22) + 1;

            Log::l1() << Log::tm() << "-analytical " << prm->threadId
                      << ": TPC-H " << q << "\n";
            queries->executeTPCH(q);
            query++;
        }
    }
    if (*(prm->runState) == 2) {
        Log::l1() << Log::tm() << "-analytical " << prm->threadId
                  << ": start test\n";
        while (*(prm->runState) == 2) {
            q = (query % 22) + 1;

            Log::l1() << Log::tm() << "-analytical " << prm->threadId
                      << ": TPC-H " << q << "\n";
            b = queries->executeTPCH(q);
            aStat->executeTPCHSuccess(q, b);
            query++;
        }
    }

    Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": exit\n";
    return NULL;
}

static void* transactionalThread(void* args) {

    threadParameters* prm = (threadParameters*)args;
    TransactionalStatistic* tStat = (TransactionalStatistic*)prm->stat;

    Transactions* transactions = new Transactions(prm->warehouseCount);
    if (!transactions->prepareStatements(prm->hDBC)) {
        exit(1);
    }

    bool b;
    int decision = 0;

    if (DbcTools::autoCommitOff(prm->hDBC)) {

        pthread_barrier_wait(prm->barStart);

        if (*(prm->runState) == 1) {
            Log::l1() << Log::tm() << "-transactional " << prm->threadId
                      << ": start warmup\n";
            while (*(prm->runState) == 1) {
                DataSource::randomUniformInt(1, 100, decision);
                if (decision <= 44 && (*(prm->runState) == 1)) {
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId
                              << ": NewOrder\n";
                    transactions->executeNewOrder(prm->hDBC);
                }
                DataSource::randomUniformInt(1, 100, decision);
                if (decision <= 44 && (*(prm->runState) == 1)) {
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId
                              << ": Payment\n";
                    transactions->executePayment(prm->hDBC);
                }
                DataSource::randomUniformInt(1, 100, decision);
                if (decision <= 4 && (*(prm->runState) == 1)) {
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId
                              << ": OrderStatus\n";
                    transactions->executeOrderStatus(prm->hDBC);
                }
                DataSource::randomUniformInt(1, 100, decision);
                if (decision <= 4 && (*(prm->runState) == 1)) {
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId
                              << ": Delivery\n";
                    transactions->executeDelivery(prm->hDBC);
                }
                DataSource::randomUniformInt(1, 100, decision);
                if (decision <= 4 && (*(prm->runState) == 1)) {
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId
                              << ": StockLevel\n";
                    transactions->executeStockLevel(prm->hDBC);
                }
            }
        }

        if (*(prm->runState) == 2) {
            Log::l1() << Log::tm() << "-transactional " << prm->threadId
                      << ": start test\n";
            while (*(prm->runState) == 2) {
                DataSource::randomUniformInt(1, 100, decision);
                if (decision <= 44 && (*(prm->runState) == 2)) {
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId
                              << ": NewOrder\n";
                    b = transactions->executeNewOrder(prm->hDBC);
                    tStat->executeTPCCSuccess(1, b);
                }
                DataSource::randomUniformInt(1, 100, decision);
                if (decision <= 44 && (*(prm->runState) == 2)) {
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId
                              << ": Payment\n";
                    b = transactions->executePayment(prm->hDBC);
                    tStat->executeTPCCSuccess(2, b);
                }
                DataSource::randomUniformInt(1, 100, decision);
                if (decision <= 4 && (*(prm->runState) == 2)) {
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId
                              << ": OrderStatus\n";
                    b = transactions->executeOrderStatus(prm->hDBC);
                    tStat->executeTPCCSuccess(3, b);
                }
                DataSource::randomUniformInt(1, 100, decision);
                if (decision <= 4 && (*(prm->runState) == 2)) {
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId
                              << ": Delivery\n";
                    b = transactions->executeDelivery(prm->hDBC);
                    tStat->executeTPCCSuccess(4, b);
                }
                DataSource::randomUniformInt(1, 100, decision);
                if (decision <= 4 && (*(prm->runState) == 2)) {
                    Log::l1() << Log::tm() << "-transactional " << prm->threadId
                              << ": StockLevel\n";
                    b = transactions->executeStockLevel(prm->hDBC);
                    tStat->executeTPCCSuccess(5, b);
                }
            }
        }
    }

    Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": exit\n";
    return NULL;
}

static int parseInt(const char* context, const char* v) {
    try {
        return std::stoi(optarg);
    } catch (std::exception) {
        errx(1, "unable to parse integer %s for %s\n", v, context);
    }
}

static void usage() {
    fprintf(stderr, "usage: chBenchmark [--warehouses N] [--out-dir PATH] gen\n"
                    "   or: chBenchmark [options] run\n");
}

static int detectWarehouses(SQLHSTMT& hStmt, int* countOut) {
    *countOut = 0;

    DbcTools::executeServiceStatement(
        hStmt, DialectStrategy::getInstance()->getSelectCountWarehouse());

    int temp = 0;
    SQLLEN nIdicator = 0;
    SQLCHAR buf[1024] = {0};
    DbcTools::fetch(hStmt, buf, &nIdicator, 1, temp);
    *countOut = temp;
    if (*countOut == 0) {
        Log::l2() << Log::tm() << "-detecting warehouses failed\n";
        return 1;
    }
    Log::l1() << Log::tm() << "-detected warehouses: " << *countOut << "\n";

    return 0;
}

static int run(int argc, char* argv[]) {
    static struct option longOpts[] = {
        {"dsn", required_argument, nullptr, 'd'},
        {"username", required_argument, nullptr, 'u'},
        {"password", required_argument, nullptr, 'p'},
        {"analytic-threads", required_argument, nullptr, 'a'},
        {"transactional-threads", required_argument, nullptr, 't'},
        {"warmup-seconds", required_argument, nullptr, 'w'},
        {"run-seconds", required_argument, nullptr, 'r'},
        {"gen-dir", required_argument, nullptr, 'g'},
        {"log-file", required_argument, nullptr, 'l'},
        {nullptr, 0, nullptr, 0}};

    int c;
    const char* dsn = NULL;
    const char* username = NULL;
    const char* password = NULL;
    int analyticThreads = 0;
    int transactionalThreads = 0;
    int warmupSeconds = 0;
    int runSeconds = 10;
    std::string genDir = "gen";
    const char* logFile = NULL;
    while ((c = getopt_long(argc, argv, "d:u:p:a:t:w:r:g:o:", longOpts,
                            nullptr)) != -1) {
        switch (c) {
        case 'd':
            dsn = optarg;
            break;
        case 'u':
            username = optarg;
            break;
        case 'p':
            password = optarg;
            break;
        case 'a':
            analyticThreads = parseInt("analytic threads", optarg);
            break;
        case 't':
            transactionalThreads = parseInt("transactional threads", optarg);
            break;
        case 'w':
            warmupSeconds = parseInt("warmup seconds", optarg);
            break;
        case 'r':
            runSeconds = parseInt("run seconds", optarg);
            break;
        case 'g':
            genDir = optarg;
            break;
        case 'l':
            logFile = optarg;
            break;
        default:
            return 1;
        }
    }
    argc -= optind;
    argv += optind;

    if (!dsn)
        errx(1, "data source name (DSN) must be specified");
    if (analyticThreads < 0)
        errx(1, "analytic threads cannot be negative");
    if (transactionalThreads < 0)
        errx(1, "transactional threads cannot be negative");
    if (analyticThreads == 0 && transactionalThreads == 0)
        errx(
            1,
            "at least one analytic or transactional thread must be configured");
    if (warmupSeconds < 0)
        errx(1, "warmup seconds cannot be negative");
    if (runSeconds < 0)
        errx(1, "run seconds cannot be negative");

    if (logFile)
        Log::open(logFile);

    // Initialization
    Log::l2() << Log::tm() << "Databasesystem:\n-initializing\n";

    // connect to DBS
    SQLHENV hEnv = nullptr;
    DbcTools::setEnv(hEnv);
    SQLHDBC hDBC = nullptr;
    if (!DbcTools::connect(hEnv, hDBC, dsn, username, password)) {
        return 1;
    }

    // create a statement handle for initializing DBS
    SQLHSTMT hStmt = 0;
    SQLAllocHandle(SQL_HANDLE_STMT, hDBC, &hStmt);

    // create database schema
    Log::l2() << Log::tm() << "Schema creation:\n";
    if (!Schema::createSchema(hStmt)) {
        return 1;
    }

    // import initial database from csv files
    Log::l2() << Log::tm() << "CSV import:\n";
    {
        char* genDirAbs = realpath(genDir.c_str(), NULL);
        genDir = genDirAbs;
        free(genDirAbs);
    }
    if (!Schema::importCSV(hStmt, genDir)) {
        return 1;
    }

    // detect warehouse count of loaded initial database
    int warehouseCount;
    if (detectWarehouses(hStmt, &warehouseCount))
        return 1;

    // perform a check to ensure that initial database was imported
    // correctly
    if (!Schema::check(hStmt)) {
        return 1;
    }

    // fire additional preparation statements
    Log::l2() << Log::tm() << "Additional Preparation:\n";
    if (!Schema::additionalPreparation(hStmt)) {
        return 1;
    }

    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    SQLDisconnect(hDBC);
    SQLFreeHandle(SQL_HANDLE_DBC, hDBC);

    DataSource::initialize(warehouseCount);

    int runState = 0; // 0=dont_run 1=warmup 2=run
    unsigned int count = analyticThreads + transactionalThreads + 1;
    pthread_barrier_t barStart;
    pthread_barrier_init(&barStart, NULL, count);

    // start analytical threads and create a statistic object for each
    // thread
    AnalyticalStatistic* aStat[analyticThreads];
    pthread_t apt[analyticThreads];
    threadParameters aprm[analyticThreads];
    for (int i = 0; i < analyticThreads; i++) {
        aStat[i] = new AnalyticalStatistic();
        aprm[i] = {&barStart, &runState,       i + 1,
                   0,         (void*)aStat[i], warehouseCount};
        if (!DbcTools::connect(hEnv, aprm[i].hDBC, dsn, username, password)) {
            exit(1);
        }
        pthread_create(&apt[i], NULL, analyticalThread, &aprm[i]);
    }

    // start transactional threads and create a statistic object for each
    // thread
    TransactionalStatistic* tStat[transactionalThreads];
    pthread_t tpt[transactionalThreads];
    threadParameters tprm[transactionalThreads];
    for (int i = 0; i < transactionalThreads; i++) {
        tStat[i] = new TransactionalStatistic();
        tprm[i] = {&barStart, &runState,       i + 1,
                   0,         (void*)tStat[i], warehouseCount};
        if (!DbcTools::connect(hEnv, tprm[i].hDBC, dsn, username, password)) {
            exit(1);
        }
        pthread_create(&tpt[i], NULL, transactionalThread, &tprm[i]);
    }

    runState = 1;
    Log::l2() << Log::tm() << "Wait for threads to initialize:\n";
    pthread_barrier_wait(&barStart);
    Log::l2() << Log::tm() << "-all threads initialized\n";

    // main test execution
    Log::l2() << Log::tm() << "Workload:\n";
    Log::l2() << Log::tm() << "-start warmup\n";
    sleep(warmupSeconds);

    runState = 2;
    Log::l2() << Log::tm() << "-start test\n";
    sleep(runSeconds);

    Log::l2() << Log::tm() << "-stop\n";
    runState = 0;

    // write results to file
    unsigned long long analyticalResults = 0;
    unsigned long long transcationalResults = 0;
    for (int i = 0; i < analyticThreads; i++) {
        aStat[i]->addResult(analyticalResults);
    }
    for (int i = 0; i < transactionalThreads; i++) {
        tStat[i]->addResult(transcationalResults);
    }

    unsigned long long qphh = analyticalResults * 3600 / runSeconds;
    unsigned long long tpmc = transcationalResults * 60 / runSeconds;

    printf("System under test:      %s\n", dsn);
    printf("Warehouses:             %d\n", warehouseCount);
    printf("Analytical threads:     %d\n", analyticThreads);
    printf("Transactional threads:  %d\n", transactionalThreads);
    printf("Warmup seconds:         %d\n", warmupSeconds);
    printf("Run seconds:            %d\n", runSeconds);
    printf("\n");
    printf("OLAP throughput [QphH]: %llu\n", qphh);
    printf("OLTP throughput [tpmC]: %llu\n", tpmc);

    Log::l2() << Log::tm()
              << "Wait for clients to return from database calls:\n";
    for (int i = 0; i < analyticThreads; i++) {
        pthread_join(apt[i], NULL);
    }
    for (int i = 0; i < transactionalThreads; i++) {
        pthread_join(tpt[i], NULL);
    }

    Log::l2() << Log::tm() << "-finished\n";

    return 0;
}

static int gen(int argc, char* argv[]) {
    static struct option longOpts[] = {
        {"warehouses", required_argument, nullptr, 'w'},
        {"out-dir", required_argument, nullptr, 'o'},
        {nullptr, 0, nullptr, 0}};

    int c;
    int warehouseCount = 1;
    const char* outDir = "gen";
    while ((c = getopt_long(argc, argv, "w:o:", longOpts, nullptr)) != -1) {
        switch (c) {
        case 'w':
            warehouseCount = parseInt("warehouse count", optarg);
            break;
        case 'o':
            outDir = optarg;
            break;
        default:
            return 1;
        }
    }
    argc -= optind;
    argv += optind;

    if (warehouseCount < 1) {
        errx(1, "warehouse count must be greater than zero");
    }

    DataSource::initialize(warehouseCount);
    TupleGen::openOutputFiles(outDir);

    for (int iId = 1; iId <= 100000; iId++) {
        // Item
        TupleGen::genItem(iId);
    }

    int oId;
    int olCount;
    std::string customerTime = "";
    std::string orderTime;
    for (int wId = 1; wId <= warehouseCount; wId++) {
        // Warehouse
        TupleGen::genWarehouse(wId);

        for (int iId = 1; iId <= 100000; iId++) {
            // Stock
            TupleGen::genStock(iId, wId);
        }

        for (int dId = 1; dId <= 10; dId++) {
            // District
            TupleGen::genDistrict(dId, wId);
            for (int cId = 1; cId <= 3000; cId++) {
                // Customer
                if (customerTime == "")
                    customerTime = DataSource::getCurrentTimeString();
                TupleGen::genCustomer(cId, dId, wId, customerTime);

                // History
                TupleGen::genHistory(cId, dId, wId);

                // Order
                oId = DataSource::permute(cId, 1, 3000);
                olCount = DataSource::nextOderlineCount();
                orderTime = DataSource::getCurrentTimeString();
                TupleGen::genOrder(oId, dId, wId, cId, olCount, orderTime);

                for (int olNumber = 1; olNumber <= olCount; olNumber++) {
                    // Orderline
                    TupleGen::genOrderline(oId, dId, wId, olNumber, orderTime);
                }

                // Neworder
                if (oId > 2100) {
                    TupleGen::genNeworder(oId, dId, wId);
                }
            }
        }
    }

    // Region
    for (int rId = 0; rId < 5; rId++) {
        TupleGen::genRegion(rId, DataSource::getRegion(rId));
    }

    // Nation
    for (int i = 0; i < 62; i++) {
        TupleGen::genNation(DataSource::getNation(i));
    }

    // Supplier
    for (int suId = 0; suId < 10000; suId++) {
        TupleGen::genSupplier(suId);
    }

    TupleGen::closeOutputFiles();

    // TODO(benesch): check for write errors.

    return 0;
}

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        if (argv[i][0] == '-')
            continue;
        else if (strcmp(argv[i], "run") == 0)
            return run(argc, argv);
        else if (strcmp(argv[i], "gen") == 0)
            return gen(argc, argv);
        else if (strcmp(argv[i], "version") == 0) {
            fprintf(stderr, "chBenchmark 0.1.0\n");
            return 0;
        } else
            errx(1, "unknown command: %s\n", argv[i]);
    }
    usage();
    return 0;
}
