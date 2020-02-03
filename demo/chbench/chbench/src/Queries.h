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

#ifndef QUERIES_H
#define QUERIES_H

#include "Dialect.h"

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>

class Queries {

  private:
    SQLHSTMT odbc_queries[22] = {0};

  public:
    bool prepareStatements(Dialect* dialect, SQLHDBC& hDBC);
    bool executeTPCH(int& i);
};

#endif
