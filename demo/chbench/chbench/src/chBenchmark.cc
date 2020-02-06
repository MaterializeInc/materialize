/*
Copyright 2014 Florian Wolf, SAP AG
Modifications Copyright 2019 Materialize, Inc.

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

#include <pqxx/pqxx>
#include "materialized.h"
#include "AnalyticalStatistic.h"
#include "DataSource.h"
#include "DbcTools.h"
#include "Dialect.h"
#include "Log.h"
#include "PthreadShim.h"
#include "Queries.h"
#include "Random.h"
#include "Schema.h"
#include "TransactionalStatistic.h"
#include "Transactions.h"
#include "TupleGen.h"
#include "mz-config.h"
#include "Histogram.h"

#include <atomic>
#include <err.h>
#include <iostream>
#include <getopt.h>
#include <pthread.h>
#include <sql.h>
#include <sqltypes.h>
#include <unistd.h>
#include <climits>
#include <thread>
#include <vector>
#include <utility>
#include <assert.h>
#include <future>
#include <cinttypes>
#include <libconfig.h++>


enum class RunState {
    off,
    warmup,
    run,
};

typedef struct {
    pthread_barrier_t* barStart;
    std::atomic<RunState>& runState;
    int threadId;
    SQLHDBC hDBC;
    void* stat;
    int warehouseCount;
    useconds_t sleepMin;
    useconds_t sleepMax;
    mz::Config* cfg;
} threadParameters;

static void* analyticalThread(void* args) {
    auto prm = (threadParameters*) args;
    auto aStat = (AnalyticalStatistic*) prm->stat;

    bool b;
    int query = 0;
    int q = 0;

    Queries queries;
    if (!queries.prepareStatements(prm->cfg->dialect, prm->hDBC)) {
        exit(1);
    }

    pthread_barrier_wait(prm->barStart);

    Log::l1() << Log::tm() << "-analytical " << prm->threadId
              << ":  start warmup\n";
    while (prm->runState == RunState::warmup) {
        q = (query % 22) + 1;

        Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": TPC-H "
                  << q << "\n";
        queries.executeTPCH(q);
        query++;
        auto sleepTime = chRandom::uniformInt(prm->sleepMin, prm->sleepMax);
        usleep(sleepTime);
    }

    Log::l1() << Log::tm() << "-analytical " << prm->threadId
              << ": start test\n";
    while (prm->runState == RunState::run) {
        q = (query % 22) + 1;

        Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": TPC-H "
                  << q << "\n";
        b = queries.executeTPCH(q);
        aStat->executeTPCHSuccess(q, b);
        query++;
        auto sleepTime = chRandom::uniformInt(prm->sleepMin, prm->sleepMax);
        usleep(sleepTime);
    }

    Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": exit\n";
    return nullptr;
}

static void peekThread(const mz::Config* pConfig, const std::atomic<RunState> *pRunState,
        std::promise<std::vector<Histogram>> promHist, useconds_t sleepMin, useconds_t sleepMax) {
    const mz::Config& config = *pConfig;
    const std::atomic<RunState>& runState = *pRunState;
    pqxx::connection c(config.materializedUrl);
    // FIXME
    assert(!config.hQueries.empty());
    size_t iQuery = 0;
    size_t size = config.hQueries.size();
    while (runState == RunState::warmup) {
        const auto& q = config.hQueries[iQuery];
        mz::peekView(c, q->first, q->second.order, q->second.limit);
        iQuery = (iQuery + 1) % size;
        auto sleepTime = chRandom::uniformInt(sleepMin, sleepMax);
        usleep(sleepTime);
    }
    std::vector<Histogram> hists;
    hists.resize(size);
    while (runState == RunState::run) {
        const auto& q = config.hQueries[iQuery];
        auto latency = mz::peekView(c, q->first, q->second.order, q->second.limit).latency;
        hists[iQuery].increment(latency.count());
        iQuery = (iQuery + 1) % size;
        auto sleepTime = chRandom::uniformInt(sleepMin, sleepMax);
        usleep(sleepTime);
    }
    promHist.set_value(std::move(hists));
}

static void flushThread(useconds_t sleepTime, const std::atomic<RunState> *pRunState) {
    const auto& runState = *pRunState;
    while (runState != RunState::run) {
        usleep(sleepTime);
    }
    while (runState == RunState::run) {
        usleep(sleepTime);
        int result = system("flush-tables");
        if (result == -1) {
            fprintf(stderr, "Failed to execute flush-tables command: %s\n", strerror(errno));
        } else if (result != 0) {
            fprintf(stderr, "flush-tables exited with failure status %d\n", result);
        } else {
            printf("flush-tables finished with successful status.");
        }
    }
}
static void materializeThread(mz::Config config, std::promise<std::vector<Histogram>> promHist,
                              int peekConns, const std::atomic<RunState> *pRunState,
                              std::optional<useconds_t> flushSleepTime, useconds_t peekMin, useconds_t peekMax) {
    const auto& connUrl = config.materializedUrl;
    auto& expected = config.expectedSources;
    const auto& kafkaUrl = config.kafkaUrl;
    const auto& schemaRegistryUrl = config.schemaRegistryUrl;
    const auto& pattern = config.viewPattern;
    pqxx::connection c(connUrl);

    while (!expected.empty()) {
        std::unordered_set<std::string> nextExpected;
        for (const auto& source: expected) {
            bool created = mz::createSource(c, kafkaUrl, schemaRegistryUrl, source);
            if (created) {
                std::cout << "Created source: " << source << std::endl;
            } else {
                nextExpected.insert(source);
            }
        }
        expected = std::move(nextExpected);
        if (!expected.empty()) {
            fprintf(stderr, "Not all sources created; sleeping for 1s and trying again.\n");
            sleep(1);
        }
    }
    std::cout << "Done creating expected sources" << std::endl;
    for (const auto& vp: config.hQueries) {
        std::cout << "Creating query " << vp->first << std::endl;
        mz::createMaterializedView(c, vp->first, vp->second.query);
    }
    if (flushSleepTime) {
        std::thread(flushThread, flushSleepTime.value(), pRunState).detach();
    }
    if (peekConns) {
        std::vector<Histogram> hists;
        hists.resize(config.hQueries.size());
        std::vector<std::future<std::vector<Histogram>>> futs;
        for (int i = 0; i < peekConns; ++i) {
            std::promise<std::vector<Histogram>> prom;
            futs.push_back(prom.get_future());
            std::thread(peekThread, &config, pRunState, std::move(prom), peekMin, peekMax).detach();
        }
        for (auto& fut: futs) {
            auto threadHists = fut.get();
            assert(hists.size() == threadHists.size());
            for (size_t i = 0; i < hists.size(); ++i) {
                assert(threadHists[i].getCounts().back() != 0);
                hists[i] += threadHists[i];
            }
        }
        promHist.set_value(std::move(hists));
    }
}

static void* transactionalThread(void* args) {
    threadParameters* prm = (threadParameters*) args;
    TransactionalStatistic* tStat = (TransactionalStatistic*) prm->stat;

    Transactions transactions {prm->warehouseCount};
    if (!transactions.prepareStatements(prm->cfg->dialect, prm->hDBC)) {
        exit(1);
    }

    bool b;

    auto& cfg = *prm->cfg;

    if (DbcTools::autoCommitOff(prm->hDBC)) {

        pthread_barrier_wait(prm->barStart);

        Log::l1() << Log::tm() << "-transactional " << prm->threadId
                  << ": start warmup\n";
        while (prm->runState == RunState::warmup) {
            auto decision = chRandom::uniformInt(1, 100);

            if (decision <= 44) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": NewOrder\n";
                transactions.executeNewOrder(prm->cfg->dialect, prm->hDBC, cfg);
            }
            else if (decision <= 88) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": Payment\n";
                transactions.executePayment(prm->cfg->dialect, prm->hDBC, cfg);
            }
            else if (decision <= 92) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": OrderStatus\n";
                transactions.executeOrderStatus(prm->cfg->dialect, prm->hDBC);
            }
            else if (decision <= 96) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": Delivery\n";
                transactions.executeDelivery(prm->cfg->dialect, prm->hDBC, cfg);
            }
            else {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": StockLevel\n";
                transactions.executeStockLevel(prm->cfg->dialect, prm->hDBC);
            }
            auto sleepTime = chRandom::uniformInt(prm->sleepMin, prm->sleepMax);
            usleep(sleepTime);
        }

        Log::l1() << Log::tm() << "-transactional " << prm->threadId
                  << ": start test\n";
        while (prm->runState == RunState::run) {
            auto decision = chRandom::uniformInt(1, 100);
            if (decision <= 44) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": NewOrder\n";
                b = transactions.executeNewOrder(prm->cfg->dialect, prm->hDBC, cfg);
                tStat->executeTPCCSuccess(1, b);
            }
            else if (decision <= 88) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": Payment\n";
                b = transactions.executePayment(prm->cfg->dialect, prm->hDBC, cfg);
                tStat->executeTPCCSuccess(2, b);
            }
            else if (decision <= 92) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": OrderStatus\n";
                b = transactions.executeOrderStatus(prm->cfg->dialect, prm->hDBC);
                tStat->executeTPCCSuccess(3, b);
            }
            else if (decision <= 96) {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": Delivery\n";
                b = transactions.executeDelivery(prm->cfg->dialect, prm->hDBC, cfg);
                tStat->executeTPCCSuccess(4, b);
            }
            else {
                Log::l1() << Log::tm() << "-transactional " << prm->threadId
                          << ": StockLevel\n";
                b = transactions.executeStockLevel(prm->cfg->dialect, prm->hDBC);
                tStat->executeTPCCSuccess(5, b);
            }
            auto sleepTime = chRandom::uniformInt(prm->sleepMin, prm->sleepMax);
            usleep(sleepTime);
        }
    }

    Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": exit\n";
    return nullptr;
}

static int parseInt(const char* context, const char* v) {
    try {
        return std::stoi(optarg);
    } catch (const std::exception&) {
        errx(1, "unable to parse integer %s for %s\n", v, context);
    }
}

static double parseDouble(const char* context, const char* v) {
    try {
        return std::stod(optarg);
    } catch (const std::exception&) {
        errx(1, "unable to parse double %s for %s\n", v, context);
    }
}

static std::vector<std::string> parseCommaSeparated(const char* v) {
    std::stringstream ss(v);
    std::string part;
    std::vector<std::string> ret;
    while (std::getline(ss, part, ',')) {
        ret.push_back(std::move(part));
    }
    return ret;
}

static void usage() {
    fprintf(stderr, "usage: chBenchmark [--warehouses N] [--out-dir PATH] gen\n"
                    "   or: chBenchmark [options] run\n");
}

static int detectWarehouses(Dialect* dialect, SQLHSTMT& hStmt, int* countOut) {
    *countOut = 0;

    DbcTools::executeServiceStatement(
        hStmt, dialect->getSelectCountWarehouse());

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

enum LongOnlyOpts {
    MIN_DELAY,
    MAX_DELAY,
    MZ_SOURCES,
    MZ_VIEWS,
    PEEK_CONNS,
    PEEK_MIN_DELAY,
    PEEK_MAX_DELAY,
    FLUSH_EVERY,
    MZ_URL,
    KAFKA_URL,
    SCHEMA_REGISTRY_URL,
    CONFIG_FILE_PATH,
};

static int run(int argc, char* argv[]) {
    int longopt_idx;
    struct option longOpts[] = {
        {"dsn", required_argument, nullptr, 'd'},
        {"username", required_argument, nullptr, 'u'},
        {"password", required_argument, nullptr, 'p'},
        {"analytic-threads", required_argument, nullptr, 'a'},
        {"transactional-threads", required_argument, nullptr, 't'},
        {"warmup-seconds", required_argument, nullptr, 'w'},
        {"run-seconds", required_argument, nullptr, 'r'},
        {"gen-dir", required_argument, nullptr, 'g'},
        {"log-file", required_argument, nullptr, 'l'},
        {"min-delay", required_argument, &longopt_idx, MIN_DELAY},
        {"max-delay", required_argument, &longopt_idx, MAX_DELAY},
        {"mz-sources", no_argument, &longopt_idx, MZ_SOURCES},
        {"mz-views", required_argument, &longopt_idx, MZ_VIEWS},
        {"peek-conns", required_argument, &longopt_idx, PEEK_CONNS},
        {"peek-min-delay", required_argument, &longopt_idx, PEEK_MIN_DELAY},
        {"peek-max-delay", required_argument, &longopt_idx, PEEK_MAX_DELAY},
        {"flush-every", required_argument, &longopt_idx, FLUSH_EVERY},
        {"mz-url", required_argument, &longopt_idx, MZ_URL},
        {"kafka-url", required_argument, &longopt_idx, KAFKA_URL},
        {"schema-registry-url", required_argument, &longopt_idx, SCHEMA_REGISTRY_URL},
        {"config-file-path", required_argument, &longopt_idx, CONFIG_FILE_PATH},
        {nullptr, 0, nullptr, 0}};

    int c;
    const char* dsn = nullptr;
    const char* username = nullptr;
    const char* password = nullptr;
    int analyticThreads = 5;
    int transactionalThreads = 10;
    int warmupSeconds = 0;
    int runSeconds = 10;
    int peekConns = 0;
    double minDelay = 0;
    double maxDelay = 0;
    double peekMinDelay = 0;
    double peekMaxDelay = 0;
    std::string genDir = "gen";
    const char* logFile = nullptr;
    bool createSources = false;
    std::vector<std::string> mzViews;
    double flushSleepTime = 0;
    std::optional<std::string> materializedUrl, kafkaUrl, schemaRegistryUrl;
    std::optional<mz::Config> config;

    while ((c = getopt_long(argc, argv, "d:u:p:a:t:w:r:g:o:l:", longOpts,
                            nullptr)) != -1) {
        if (c == 0) switch (longopt_idx) {
        case MIN_DELAY:
            minDelay = parseDouble("minimum delay between queries (s)", optarg);
            break;
        case MAX_DELAY:
            maxDelay = parseDouble("maximum delay between queries (s)", optarg);
            break;
        case MZ_SOURCES:
            createSources = true;
            break;
        case MZ_VIEWS:
            mzViews = parseCommaSeparated(optarg);
            break;
        case PEEK_CONNS:
            peekConns = parseInt("Materialized peek threads", optarg);
            break;
        case PEEK_MIN_DELAY:
            peekMinDelay = parseDouble("minimum delay between peeks (s)", optarg);
            break;
        case PEEK_MAX_DELAY:
            peekMaxDelay = parseDouble("maximum delay between peeks (s)", optarg);
            break;
        case FLUSH_EVERY:
            flushSleepTime = parseDouble("Interval to run flush-tables hack (s)", optarg);
            break;
        case MZ_URL:
            materializedUrl = optarg;
            break;
        case KAFKA_URL:
            kafkaUrl = optarg;
            break;
        case SCHEMA_REGISTRY_URL:
            schemaRegistryUrl = optarg;
            break;
        case CONFIG_FILE_PATH: {
            libconfig::Config lc_config;
            lc_config.readFile(optarg);
            config = Config::get_config(lc_config);
            break;
        }
        default:
            return 1;
        }
        else switch (c) {
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
    if (!config) {
        config = mz::defaultConfig();
    }
    mz::Config mzCfg = std::move(*config);
    if (materializedUrl) {
        mzCfg.materializedUrl = *materializedUrl;
    }
    if (kafkaUrl) {
        mzCfg.kafkaUrl = *kafkaUrl;
    }
    if (schemaRegistryUrl) {
        mzCfg.schemaRegistryUrl = *schemaRegistryUrl;
    }
    argc -= optind;
    argv += optind;

    if (minDelay < 0.0 || maxDelay < 0.0 || minDelay > maxDelay
        || minDelay * 1'000'000 > UINT_MAX || maxDelay * 1'000'000 > UINT_MAX
        || peekMinDelay < 0.0 || peekMaxDelay < 0.0 || peekMinDelay > peekMaxDelay
        || peekMinDelay * 1'000'000 > UINT_MAX || peekMaxDelay * 1'000'000 > UINT_MAX
        || flushSleepTime < 0.0 || flushSleepTime * 1'000'000 > UINT_MAX)
        errx(1, "Invalid time bounds specified");
    if (!dsn)
        errx(1, "data source name (DSN) must be specified");
    if (analyticThreads < 0)
        errx(1, "analytic threads cannot be negative");
    if (transactionalThreads < 0)
        errx(1, "transactional threads cannot be negative");
    if (warmupSeconds < 0)
        errx(1, "warmup seconds cannot be negative");
    if (runSeconds < 0)
        errx(1, "run seconds cannot be negative");
    if (!mzViews.empty() && !createSources)
        errx(1, "--mz-views requires --mz-sources");
    if (peekConns < 0)
        errx(1, "peek threads cannot be negative");

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
    if (!Schema::createSchema(mzCfg.dialect, hStmt)) {
        return 1;
    }

    // import initial database from csv files
    Log::l2() << Log::tm() << "CSV import:\n";
    if (!Schema::importCSV(mzCfg.dialect, hStmt, genDir)) {
        return 1;
    }

    // detect warehouse count of loaded initial database
    int warehouseCount;
    if (detectWarehouses(mzCfg.dialect, hStmt, &warehouseCount))
        return 1;

    // perform a check to ensure that initial database was imported
    // correctly
    if (!Schema::check(mzCfg.dialect, hStmt)) {
        return 1;
    }

    // fire additional preparation statements
    Log::l2() << Log::tm() << "Additional Preparation:\n";
    if (!Schema::additionalPreparation(mzCfg.dialect, hStmt)) {
        return 1;
    }

    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    SQLDisconnect(hDBC);
    SQLFreeHandle(SQL_HANDLE_DBC, hDBC);

    DataSource::initialize(warehouseCount);

    std::atomic<RunState> runState {RunState::off};
    unsigned int count = analyticThreads + transactionalThreads + 1;
    pthread_barrier_t barStart;
    pthread_barrier_init(&barStart, nullptr, count);

    // start analytical threads and create a statistic object for each
    // thread
    AnalyticalStatistic* aStat[analyticThreads];
    pthread_t apt[analyticThreads];
    std::vector<threadParameters> aprm;
    aprm.reserve(analyticThreads);
    for (int i = 0; i < analyticThreads; i++) {
        aStat[i] = new AnalyticalStatistic();
        aprm.push_back(
            {&barStart, runState, i + 1, 0, (void*) aStat[i], warehouseCount, (unsigned)(minDelay * 1'000'000), (unsigned)(maxDelay * 1'000'000), &mzCfg});
        if (!DbcTools::connect(hEnv, aprm[i].hDBC, dsn, username, password)) {
            exit(1);
        }
        pthread_create(&apt[i], nullptr, analyticalThread, &aprm[i]);
    }

    // start transactional threads and create a statistic object for each
    // thread
    TransactionalStatistic* tStat[transactionalThreads];
    pthread_t tpt[transactionalThreads];
    std::vector<threadParameters> tprm;
    tprm.reserve(transactionalThreads);
    for (int i = 0; i < transactionalThreads; i++) {
        tStat[i] = new TransactionalStatistic();
        tprm.push_back(
            {&barStart, runState, i + 1, 0, (void*) tStat[i], warehouseCount, (unsigned)(minDelay * 1'000'000), (unsigned)(maxDelay * 1'000'000), &mzCfg});
        if (!DbcTools::connect(hEnv, tprm[i].hDBC, dsn, username, password)) {
            exit(1);
        }
        pthread_create(&tpt[i], nullptr, transactionalThread, &tprm[i]);
    }
    std::promise<std::vector<Histogram>> promHist;
    auto futHist = promHist.get_future();
    runState = RunState::warmup;
    if (createSources) {
        for (const auto& view: mzViews) {
            if (auto i = mzCfg.allQueries.find(view); i != mzCfg.allQueries.end()) {
                mzCfg.hQueries.emplace_back(&*i);
            } else {
                errx(1, "No such view: %s", view.c_str());
            }
        }
        if (mzCfg.hQueries.empty() && peekConns)
            errx(1, "--peek-conns requires --mz-sources and configured views");
        // TODO - kick off peeks
        std::thread(materializeThread,
                    mzCfg,
                    std::move(promHist),
                    peekConns,
                    &runState,
                    (flushSleepTime == 0) ? std::nullopt : std::optional<useconds_t>(flushSleepTime * 1'000'000),
                    (unsigned)(peekMinDelay * 1'000'000), (unsigned)(peekMaxDelay * 1'000'000)
        ).detach();
    }

    Log::l2() << Log::tm() << "Wait for threads to initialize:\n";
    pthread_barrier_wait(&barStart);
    Log::l2() << Log::tm() << "-all threads initialized\n";

    // main test execution
    Log::l2() << Log::tm() << "Workload:\n";
    Log::l2() << Log::tm() << "-start warmup\n";
    sleep(warmupSeconds);

    runState = RunState::run;
    Log::l2() << Log::tm() << "-start test\n";
    sleep(runSeconds);

    Log::l2() << Log::tm() << "-stop\n";
    runState = RunState::off;

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
    printf("Sleep after query:      %f-%f s\n", minDelay, maxDelay);
    printf("\n");
    printf("OLAP throughput [QphH]: %llu\n", qphh);
    printf("OLTP throughput [tpmC]: %llu\n", tpmc);

    if (peekConns) {
        auto hists = futHist.get();
        printf("\n\nQuery latencies:\n");
        assert(mzCfg.hQueries.size() == hists.size());
        // for thousands separator in printf output
        setlocale(LC_NUMERIC, "en_US.UTF-8");
        for (size_t i = 0; i < hists.size(); ++i) {
            printf("\n%s:\ncount\t|\tlatency (ns)\n----------------------------\n", mzCfg.hQueries[i]->first.c_str());
            uint64_t nanos = 1;
            bool printed_first = false;
            for (auto bucketCount : hists[i].getCounts()) {
                if (bucketCount) {
                    printed_first = true;
                }
                if (printed_first) {
                    printf("%'" PRIu64 "\t|\t%'" PRIu64 "\n", bucketCount, nanos);
                }
                nanos *= 2;
            }
        }
    }

    Log::l2() << Log::tm()
              << "Wait for clients to return from database calls:\n";
    for (int i = 0; i < analyticThreads; i++) {
        pthread_join(apt[i], nullptr);
    }
    for (int i = 0; i < transactionalThreads; i++) {
        pthread_join(tpt[i], nullptr);
    }

    Log::l2() << Log::tm() << "-finished\n";

    return 0;
}

static int gen(int argc, char* argv[]) {
    int longopt_idx;
    static struct option longOpts[] = {
        {"warehouses", required_argument, nullptr, 'w'},
        {"out-dir", required_argument, nullptr, 'o'},
        {"config-file-path", required_argument, &longopt_idx, CONFIG_FILE_PATH},
        {nullptr, 0, nullptr, 0}};

    int c;
    int warehouseCount = 1;
    const char* outDir = "gen";
    std::optional<mz::Config> config;
    while ((c = getopt_long(argc, argv, "w:o:", longOpts, nullptr)) != -1) {
        if (c == 0) switch (longopt_idx) {
        case CONFIG_FILE_PATH: {
            libconfig::Config lc_config;
            lc_config.readFile(optarg);
            config = Config::get_config(lc_config);
            break;
        }
        default:
            return 1;
        } else switch (c) {
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
    if (!config) {
        config = mz::defaultConfig();
    }
    mz::Config mzCfg = std::move(*config);
    argc -= optind;
    argv += optind;

    if (warehouseCount < 1) {
        errx(1, "warehouse count must be greater than zero");
    }

    DataSource::initialize(warehouseCount);
    TupleGen::openOutputFiles(outDir);

    for (int iId = 1; iId <= 100000; iId++) {
        // Item
        TupleGen::genItem(iId, mzCfg);
    }

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
            int nextOlCount = -1;
            for (int cId = 1; cId <= 3000; cId++) {
                // Customer
                if (customerTime.empty())
                    customerTime = DataSource::getCurrentTimeString(mzCfg.order_entry_date_offset_millis(chRandom::rng));
                TupleGen::genCustomer(cId, dId, wId, customerTime);

                // History
                TupleGen::genHistory(cId, dId, wId, mzCfg);

                // Order
                int oId = DataSource::permute(cId, 1, 3000);
                int olCount;
                if (nextOlCount == -1) {
                    olCount = chRandom::uniformInt(5, 15);
                    nextOlCount = 20 - olCount;
                } else {
                    olCount = nextOlCount;
                    nextOlCount = -1;
                }

                orderTime = DataSource::getCurrentTimeString(mzCfg.order_entry_date_offset_millis(chRandom::rng));

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
