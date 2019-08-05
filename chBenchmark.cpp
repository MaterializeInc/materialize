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

#include "src/AnalyticalStatistic.h"
#include "src/Config.h"
#include "src/DataSource.h"
#include "src/DbcTools.h"
#include "src/Log.h"
#include "src/Queries.h"
#include "src/Schema.h"
#include "src/TransactionalStatistic.h"
#include "src/Transactions.h"
#include "src/TupleGen.h"

#include <fstream>
#include <iostream>
#include <pthread.h>
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <unistd.h>

using namespace std;

bool dbgen(){

	DataSource::initialize();
	TupleGen::openOutputFiles();

	for(int iId=1; iId<=100000; iId++){
		//Item
		TupleGen::genItem(iId);
	}

	int oId;
	int olCount;
	string customerTime = "";
	string orderTime;
	for(int wId=1; wId<=Config::getWarehouseCount(); wId++){
		//Warehouse
		TupleGen::genWarehouse(wId);

		for(int iId=1; iId<=100000; iId++){
			//Stock
			TupleGen::genStock(iId, wId);
		}

		for(int dId=1; dId<=10; dId++){
			//District
			TupleGen::genDistrict(dId, wId);
			for(int cId=1; cId<=3000; cId++){
				//Customer
				if(customerTime == "")
					customerTime = DataSource::getCurrentTimeString();
				TupleGen::genCustomer(cId, dId, wId,customerTime);

				//History
				TupleGen::genHistory(cId, dId, wId);

				//Order
				oId = DataSource::permute(cId,1,3000);
				olCount = DataSource::nextOderlineCount();
				orderTime = DataSource::getCurrentTimeString();
				TupleGen::genOrder(oId, dId, wId, cId, olCount, orderTime);

				for(int olNumber=1; olNumber<=olCount; olNumber++){
					//Orderline
					TupleGen::genOrderline(oId, dId, wId, olNumber, orderTime);
				}

				//Neworder
				if(oId>2100){
					TupleGen::genNeworder(oId, dId, wId);
				}
			}
		}
	}

	//Region
	for(int rId=0; rId<5; rId++){
		TupleGen::genRegion(rId,DataSource::getRegion(rId));
	}

	//Nation
	for(int i=0; i<62; i++){
		TupleGen::genNation(DataSource::getNation(i));
	}

	//Supplier
	for(int suId=0; suId<10000; suId++){
		TupleGen::genSupplier(suId);
	}

	TupleGen::closeOutputFiles();

	return 1;
}

typedef struct _threadParameters{
	pthread_barrier_t* barStart;
	int* runState;
	int threadId;
	SQLHENV* hEnv;
	void* stat;
} threadParameters;

void* analyticalThread(void* args){

	threadParameters* prm = (threadParameters*) args;
	AnalyticalStatistic* aStat = (AnalyticalStatistic*) prm->stat;

	bool b;
	long query=0;
	int q=0;

	SQLHDBC hDBC = 0;
	if(!DbcTools::connect(*(prm->hEnv), hDBC)){
		exit(1);
	}

	Queries* queries = new Queries();
	if(!queries->prepareStatements(hDBC)){
		exit(1);
	}

	pthread_barrier_wait(prm->barStart);

	if(*(prm->runState)==1){
		Log::l1() << Log::tm() << "-analytical " << prm->threadId << ":  start warmup\n";
		while(*(prm->runState)==1){
			q=(query%22)+1;

			Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": TPC-H " << q << "\n";
			queries->executeTPCH(q);
			query++;
		}
	}
	if(*(prm->runState)==2){
		Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": start test\n";
		while(*(prm->runState)==2){
			q=(query%22)+1;

			Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": TPC-H " << q << "\n";
			b = queries->executeTPCH(q);
			aStat->executeTPCHSuccess(q,b);
			query++;
		}
	}

	Log::l1() << Log::tm() << "-analytical " << prm->threadId << ": exit\n";
	return NULL;
}

void* transactionalThread(void* args){

	threadParameters* prm = (threadParameters*) args;
	TransactionalStatistic* tStat = (TransactionalStatistic*) prm->stat;

	SQLHDBC hDBC = 0;
	if(!DbcTools::connect(*(prm->hEnv), hDBC)){
		exit(1);
	}

	Transactions* transactions = new Transactions();
	if(!transactions->prepareStatements(hDBC)){
		exit(1);
	}

	bool b;
	int decision=0;

	if(DbcTools::autoCommitOff(hDBC)){

		pthread_barrier_wait(prm->barStart);

		if(*(prm->runState)==1){
			Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": start warmup\n";
			while(*(prm->runState)==1){
				DataSource::randomUniformInt(1,100,decision);
				if(decision<=44 && (*(prm->runState)==1)){
					Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": NewOrder\n";
					transactions->executeNewOrder(hDBC);
				}
				DataSource::randomUniformInt(1,100,decision);
				if(decision<=44 && (*(prm->runState)==1)){
					Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": Payment\n";
					transactions->executePayment(hDBC);
				}
				DataSource::randomUniformInt(1,100,decision);
				if(decision<=4 && (*(prm->runState)==1)){
					Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": OrderStatus\n";
					transactions->executeOrderStatus(hDBC);
				}
				DataSource::randomUniformInt(1,100,decision);
				if(decision<=4 && (*(prm->runState)==1)){
					Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": Delivery\n";
					transactions->executeDelivery(hDBC);
				}
				DataSource::randomUniformInt(1,100,decision);
				if(decision<=4 && (*(prm->runState)==1)){
					Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": StockLevel\n";
					transactions->executeStockLevel(hDBC);
				}
			}
		}

		if(*(prm->runState)==2){
			Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": start test\n";
			while(*(prm->runState)==2){
				DataSource::randomUniformInt(1,100,decision);
				if(decision<=44 && (*(prm->runState)==2)){
					Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": NewOrder\n";
					b = transactions->executeNewOrder(hDBC);
					tStat->executeTPCCSuccess(1,b);
				}
				DataSource::randomUniformInt(1,100,decision);
				if(decision<=44 && (*(prm->runState)==2)){
					Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": Payment\n";
					b = transactions->executePayment(hDBC);
					tStat->executeTPCCSuccess(2,b);
				}
				DataSource::randomUniformInt(1,100,decision);
				if(decision<=4 && (*(prm->runState)==2)){
					Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": OrderStatus\n";
					b = transactions->executeOrderStatus(hDBC);
					tStat->executeTPCCSuccess(3,b);
				}
				DataSource::randomUniformInt(1,100,decision);
				if(decision<=4 && (*(prm->runState)==2)){
					Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": Delivery\n";
					b = transactions->executeDelivery(hDBC);
					tStat->executeTPCCSuccess(4,b);
				}
				DataSource::randomUniformInt(1,100,decision);
				if(decision<=4 && (*(prm->runState)==2)){
					Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": StockLevel\n";
					b = transactions->executeStockLevel(hDBC);
					tStat->executeTPCCSuccess(5,b);
				}
			}
		}
	}

	Log::l1() << Log::tm() << "-transactional " << prm->threadId << ": exit\n";
	return NULL;
}

int main(int argc, char* argv[]){

	//Read config from comand line parameters
	if(!Config::initialize(argc,argv))
		return 1;


	//1. Creating initial database as CSV's
	if(Config::getType()==1){
		cout << "Generating initial database:" << endl;
		if(!dbgen()){
			cout << "-failed" << endl;
			return 1;
		}
		cout << "-succeeded" << endl;
	}

	//2. Run main chBenchmark
	else if(Config::getType()==2){

		//Initialization
		Log::l2() << Log::tm() << "Databasesystem:\n-initializing\n";

		//connect to DBS
		SQLHENV hEnv = 0;
		DbcTools::setEnv(hEnv);
		SQLHDBC hDBC = 0;
		if(!DbcTools::connect(hEnv, hDBC)){
			return 1;
		}

		//create a statement handle for initializing DBS
		SQLHSTMT hStmt = 0;
		SQLAllocHandle(SQL_HANDLE_STMT, hDBC, &hStmt);

		//create database schema
		Log::l2() << Log::tm() << "Schema creation:\n";
		if(!Schema::createSchema(hStmt)){
			return 1;
		}

		//import initial database from csv files
		Log::l2() << Log::tm() << "CSV import:\n";
		if(!Schema::importCSV(hStmt)){
			return 1;
		}

		//detect warehouse count of loaded initial database
		if(!Config::warehouseDetection(hStmt)){
			return 1;
		}

		//perform a check to ensure that initial database was imported correctly
		if(!Schema::check(hStmt)){
			return 1;
		}

		//fire additional preparation statements
		Log::l2() << Log::tm() << "Additional Preparation:\n";
		if(!Schema::additionalPreparation(hStmt)){
			return 1;
		}

		SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
		SQLDisconnect(hDBC);
		SQLFreeHandle(SQL_HANDLE_DBC, hDBC);

		DataSource::initialize();

		int runState = 0; //0=dont_run 1=warmup 2=run
		unsigned int count=Config::getAnalyticalClients()+Config::getTransactionalClients()+1;
		pthread_barrier_t barStart;
		pthread_barrier_init(&barStart,NULL,count);

		//start analytical threads and create a statistic object for each thread
		AnalyticalStatistic* aStat[Config::getAnalyticalClients()];
		pthread_t apt[Config::getAnalyticalClients()];
		threadParameters aprm[Config::getAnalyticalClients()];
		for(int i=0; i<Config::getAnalyticalClients(); i++){
			aStat[i] = new AnalyticalStatistic();
			aprm[i] = {&barStart,&runState,i+1,&hEnv,(void*)aStat[i]};
			pthread_create(&apt[i],NULL,analyticalThread,&aprm[i]);
		}

		//start transactional threads and create a statistic object for each thread
		TransactionalStatistic* tStat[Config::getTransactionalClients()];
		pthread_t tpt[Config::getTransactionalClients()];
		threadParameters tprm[Config::getTransactionalClients()];
		for(int i=0; i<Config::getTransactionalClients(); i++){
			tStat[i] = new TransactionalStatistic();
			tprm[i] = {&barStart,&runState,i+1,&hEnv,(void*)tStat[i]};
			pthread_create(&tpt[i],NULL,transactionalThread,&tprm[i]);
		}

		runState = 1;
		Log::l2() << Log::tm() << "Wait for threads to initialize:\n";
		pthread_barrier_wait(&barStart);
		Log::l2() << Log::tm() << "-all threads initialized\n";

		//main test execution
		Log::l2() << Log::tm() << "Workload:\n";
		Log::l2() << Log::tm() << "-start warmup\n";
		sleep(Config::getWarmupDurationInS());

		runState = 2;
		Log::l2() << Log::tm() << "-start test\n";
		sleep(Config::getTestDurationInS());

		Log::l2() << Log::tm() << "-stop\n";
		runState = 0;

		//write results to file
		unsigned long long analyticalResults=0;
		unsigned long long  transcationalResults=0;
		for(int i=0; i<Config::getAnalyticalClients(); i++){
			aStat[i]->addResult(analyticalResults);
		}
		for(int i=0; i<Config::getTransactionalClients(); i++){
			tStat[i]->addResult(transcationalResults);
		}

		unsigned long long qphh=analyticalResults*3600/Config::getTestDurationInS();
		unsigned long long tpmc=transcationalResults*60/Config::getTestDurationInS();

		ofstream resultStream;
		resultStream.open( (Config::getOutputPath()+"/Results.csv").c_str() );
		resultStream << "System Under Test;" << Config::getDataSourceName() << endl;
		resultStream << "Analytical Clients;" << Config::getAnalyticalClients() << endl;
		resultStream << "Transactional Clients;" << Config::getTransactionalClients() << endl;
		resultStream << "Warmup Duration in [s];" << Config::getWarmupDurationInS() << endl;
		resultStream << "Test Duration in [s];" << Config::getTestDurationInS() << endl;
		resultStream << "Warehouses;" << Config::getWarehouseCount() << endl;
		resultStream << endl;
		resultStream << "OLAP Throughput in [QphH];" << qphh << endl;
		resultStream << "OLTP Throughput in [tpmC];" << tpmc << endl;
		resultStream.close();
		Log::l2() << Log::tm() << "-results written to: " << Config::getOutputPath() << "/Results.csv\n";

		Log::l2() << Log::tm() << "Wait for clients to return from database calls:\n";
		for(int i=0; i<Config::getAnalyticalClients(); i++){
			pthread_join(apt[i], NULL);
		}
		for(int i=0; i<Config::getTransactionalClients(); i++){
			pthread_join(tpt[i], NULL);
		}

		Log::l2() << Log::tm() << "-finished\n";

	}
	else if(Config::getType()==3){
		cout << "Usage:"
				"\n1. Create initial database as CSV files:\n"
				"   chBenchmark\n"
				"    -csv\n"
				"    -wh <WAREHOUSE_COUNT>\n"
				"    -pa <INITIAL_DB_GEN_PATH>\n"
				"\n2. Run test:\n"
				"   chBenchmark\n"
				"    -run\n"
				"    -dsn <DATA_SOURCE_NAME>\n"
				"    -usr <DBS_USER>\n"
				"    -pwd <DBS_PASSWORD>\n"
				"    -a <ANALYTICAL_CLIENTS>\n"
				"    -t <TRANSACTIONAL_CLIENTS>\n"
				"    -wd <WARMUP_DURATION_IN_S>\n"
				"    -td <TEST_DURATION_IN_S>\n"
				"    -pa <INITIAL_DB_CREATION_PATH>\n"
				"    -op <OUTPUT_PATH>\n\n";
	}
	else{
		return 1;
	}

	return 0;
}
