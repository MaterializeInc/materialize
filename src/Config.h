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

#ifndef CONFIG_H
#define CONFIG_H

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <string>

class Config{

	private:
		static int TYPE;

		static std::string DATA_SOURCE_NAME;
		static std::string DBS_USER;
		static std::string DBS_PASSWORD;

		static int ANALYTICAL_CLIENTS;
		static int TRANSACTIONAL_CLIENTS;

		static int WARMUP_DURATION_IN_S;
		static int TEST_DURATION_IN_S;
		static int WAREHOUSE_COUNT;
		static std::string INITIAL_DB_CREATION_PATH;
		static std::string OUTPUT_PATH;
		
		static const char CSV_DELIMITER = '|';

		static int is(const char* value, int argc, char* argv[]);
		static bool is(const char* value, int argc, char* argv[], int* dest);
		static bool is(const char* value, int argc, char* argv[], std::string* dest);

	public:				
		static bool initialize(int argc, char* argv[]);
		static bool warehouseDetection(SQLHSTMT& hStmt);

		static int getType();

		static std::string getDataSourceName();
		static std::string getDbsUser();
		static std::string getDbsPassword();

		static int getAnalyticalClients();
		static int getTransactionalClients();

		static int getWarmupDurationInS();
		static int getTestDurationInS();
		static int getWarehouseCount();

		static std::string getOutputPath();
		static std::string getInitialDbCreationPath();
		static char getCsvDelim();

};

#endif
