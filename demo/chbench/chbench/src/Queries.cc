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

#include "Queries.h"

#include "DbcTools.h"
#include "Log.h"

bool Queries::prepareStatements(Dialect* dialect, SQLHDBC& hDBC) {

    for (int i = 0; i < 22; i++) {
        if (!DbcTools::allocAndPrepareStmt(
                hDBC, odbc_queries[i],
                dialect->getTpchQueryStrings()[i])) {
            Log::l2() << Log::tm() << "-prepare statements failed\n";
            return false;
        }
    }
    Log::l1() << Log::tm() << "-prepare statements succeeded\n";
    return true;
}

bool Queries::executeTPCH(int& i) {

    if (DbcTools::resetStatement(odbc_queries[i - 1])) {
        return DbcTools::executePreparedStatement(odbc_queries[i - 1]);
    }
    return false;
}
