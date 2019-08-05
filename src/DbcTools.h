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

#ifndef DBCTOOLS_H
#define DBCTOOLS_H

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <string>

class DbcTools{
	
	private:
		static bool fetch(SQLHSTMT& hStmt, SQLCHAR* buf, SQLLEN* nIdicator, int pos);
		static bool reviewReturn(SQLHANDLE& handle, SQLSMALLINT handleType, SQLRETURN& ret, bool showError=0);

	public:
		static bool setEnv(SQLHENV& hEnv);
		static bool connect(SQLHENV& hEnv, SQLHDBC& hDBC);
		static bool autoCommitOff(SQLHDBC& hDBC);
		static bool allocAndPrepareStmt(SQLHDBC& hDBC, SQLHSTMT& hStmt, const char* stmt);
		static bool resetStatement(SQLHSTMT& hStmt);
		static bool bind(SQLHSTMT& hStmt, int pos, int& value);
		static bool bind(SQLHSTMT& hStmt, int pos, double& value);
		static bool bind(SQLHSTMT& hStmt, int pos, int bufferLength, char* buffer);
		static bool bind(SQLHSTMT& hStmt, int pos, SQL_TIMESTAMP_STRUCT& ts);
		static bool executePreparedStatement(SQLHSTMT& hStmt);
		static bool executeServiceStatement(SQLHSTMT& hStmt, const char* stmt, bool showError=1);
		static bool fetch(SQLHSTMT& hStmt, SQLCHAR* buf, SQLLEN* nIdicator, int pos, std::string& value);
		static bool fetch(SQLHSTMT& hStmt, SQLCHAR* buf, SQLLEN* nIdicator, int pos, int& value);
		static bool fetch(SQLHSTMT& hStmt, SQLCHAR* buf, SQLLEN* nIdicator, int pos, double& value);
		static bool commit(SQLHDBC& hDBC);
		static bool rollback(SQLHDBC& hDBC);
};

#endif
