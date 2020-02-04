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

#ifndef TRANSACTIONS_H
#define TRANSACTIONS_H

#include "Dialect.h"

#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include "mz-config.h"

class Transactions {

  private:
    SQLHSTMT noWarehouseSelect = 0;
    SQLHSTMT noDistrictSelect = 0;
    SQLHSTMT noDistrictUpdate = 0;
    SQLHSTMT noCustomerSelect = 0;
    SQLHSTMT noItemSelect = 0;
    SQLHSTMT noStockSelects[10] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    SQLHSTMT noStockUpdates[2] = {0, 0};
    SQLHSTMT noOrderlineInsert = 0;
    SQLHSTMT noOrderInsert = 0;
    SQLHSTMT noNewOrderInsert = 0;

    SQLHSTMT pmWarehouseSelect = 0;
    SQLHSTMT pmWarehouseUpdate = 0;
    SQLHSTMT pmDistrictSelect = 0;
    SQLHSTMT pmDistrictUpdate = 0;
    SQLHSTMT pmCustomerSelect1 = 0;
    SQLHSTMT pmCustomerSelect2 = 0;
    SQLHSTMT pmCustomerSelect3 = 0;
    SQLHSTMT pmCustomerUpdate1 = 0;
    SQLHSTMT pmCustomerSelect4 = 0;
    SQLHSTMT pmCustomerUpdate2 = 0;
    SQLHSTMT pmHistoryInsert = 0;

    SQLHSTMT osCustomerSelect1 = 0;
    SQLHSTMT osCustomerSelect2 = 0;
    SQLHSTMT osCustomerSelect3 = 0;
    SQLHSTMT osOrderSelect = 0;
    SQLHSTMT osOrderlineSelect = 0;

    SQLHSTMT dlNewOrderSelect = 0;
    SQLHSTMT dlNewOrderDelete = 0;
    SQLHSTMT dlOrderSelect = 0;
    SQLHSTMT dlOrderUpdate = 0;
    SQLHSTMT dlOrderlineUpdate = 0;
    SQLHSTMT dlOrderlineSelect = 0;
    SQLHSTMT dlCustomerUpdate = 0;

    SQLHSTMT slDistrictSelect = 0;
    SQLHSTMT slStockSelect = 0;

    int warehouseCount;

    bool prepare(Dialect* dialect, SQLHDBC& hDBC);

  public:
    Transactions(int wc) : warehouseCount(wc) {}
    bool prepareStatements(Dialect* dialect, SQLHDBC& hDBC);

    bool executeNewOrder(Dialect* dialect, SQLHDBC& hDBC, mz::Config& cfg);
    bool executePayment(Dialect* dialect, SQLHDBC& hDBC, mz::Config& cfg);
    bool executeOrderStatus(Dialect* dialect, SQLHDBC& hDBC);
    bool executeDelivery(Dialect* dialect, SQLHDBC& hDBC, mz::Config& cfg);
    bool executeStockLevel(Dialect* dialect, SQLHDBC& hDBC);
};

#endif
