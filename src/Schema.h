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

#ifndef SCHEMA_H
#define SCHEMA_H

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <string>

class Schema {

  private:
    static bool check(SQLHSTMT& hStmt, const char* query, int& cnt);

  public:
    static bool createSchema(SQLHSTMT& hStmt);
    static bool importCSV(SQLHSTMT& hStmt, const std::string& genDir);
    static bool check(SQLHSTMT& hStmt);
    static bool additionalPreparation(SQLHSTMT& hStmt);
};

#endif
