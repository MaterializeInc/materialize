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

#include "Schema.h"

#include "DbcTools.h"
#include "Log.h"
#include "dialect/DialectStrategy.h"

#include <string>

bool Schema::check(SQLHSTMT& hStmt, const char* query, int& cnt) {

    SQLLEN nIdicator = 0;
    SQLCHAR buf[128] = {0};

    if (!DbcTools::executeServiceStatement(hStmt, query)) {
        Log::l2() << Log::tm() << "-determine count failed\n";
        return false;
    }
    if (!DbcTools::fetch(hStmt, buf, &nIdicator, 1, cnt)) {
        Log::l2() << Log::tm() << "-determine count failed\n";
        return false;
    }
    if (cnt == 0) {
        Log::l2() << Log::tm() << "-determine count failed\n";
        return false;
    }
    return true;
}

bool Schema::createSchema(SQLHSTMT& hStmt) {

    for (const char* stmt: DialectStrategy::getInstance()->getDropExistingSchemaStatements()) {
        DbcTools::executeServiceStatement(
            hStmt,
            stmt,
            false);
    }

    for (const char* stmt : DialectStrategy::getInstance()->getCreateSchemaStatements()) {
        if (!DbcTools::executeServiceStatement(
                hStmt, stmt)) {
            Log::l2() << Log::tm() << "-failed\n";
            return false;
        }
    }

    Log::l2() << Log::tm() << "-succeeded\n";
    return true;
}

bool Schema::importCSV(SQLHSTMT& hStmt, const std::string& genDir) {

    if (DialectStrategy::getInstance()->getImportPrefix().size() !=
        DialectStrategy::getInstance()->getImportSuffix().size()) {
        Log::l2()
            << Log::tm()
            << "-failed (different size of ImportPrefix and ImportSuffix vector in dialect class)\n";
        return false;
    }

    for (size_t i = 0;
         i < DialectStrategy::getInstance()->getImportPrefix().size(); i++) {
        if (!DbcTools::executeServiceStatement(
                hStmt,
                std::string(
                    DialectStrategy::getInstance()->getImportPrefix()[i] +
                    genDir +
                    DialectStrategy::getInstance()->getImportSuffix()[i])
                    .c_str())) {
            Log::l2() << Log::tm() << "-failed\n";
            return false;
        }
    }

    Log::l2() << Log::tm() << "-succeeded\n";
    return true;
}

bool Schema::check(SQLHSTMT& hStmt) {

    int wh = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountWarehouse(),
               wh))
        return false;

    int ds = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountDistrict(),
               ds))
        return false;
    if (ds != 10 * wh) {
        Log::l2() << Log::tm() << "-check failed (#DISTRICT: " << ds << ")\n";
        return false;
    }

    int cs = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountCustomer(),
               cs))
        return false;
    if (cs != 30000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#CUSTOMER: " << cs << ")\n";
        return false;
    }

    int od = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountOrder(),
               od))
        return false;
    if (od != 30000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#ORDER: " << od << ")\n";
        return false;
    }

    int ol = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountOrderline(),
               ol))
        return false;
    if (ol != 300000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#ORDERLINE: " << ol << ")\n";
        return false;
    }

    int no = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountNeworder(),
               no))
        return false;
    if (no != 9000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#NEWORDER: " << no << ")\n";
        return false;
    }

    int hs = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountHistory(),
               hs))
        return false;
    if (hs != 30000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#HISTORY: " << hs << ")\n";
        return false;
    }

    int st = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountStock(),
               st))
        return false;
    if (st != 100000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#STOCK: " << st << ")\n";
        return false;
    }

    int it = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountItem(), it))
        return false;
    if (it != 100000) {
        Log::l2() << Log::tm() << "-check failed (#ITEM: " << it << ")\n";
        return false;
    }

    int sp = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountSupplier(),
               sp))
        return false;
    if (sp != 10000) {
        Log::l2() << Log::tm() << "-check failed (#SUPPLIER: " << sp << ")\n";
        return false;
    }

    int na = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountNation(),
               na))
        return false;
    if (na != 62) {
        Log::l2() << Log::tm() << "-check failed (#NATION: " << na << ")\n";
        return false;
    }

    int rg = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountRegion(),
               rg))
        return false;
    if (rg != 5) {
        Log::l2() << Log::tm() << "-check failed (#REGION: " << rg << ")\n";
        return false;
    }

    Log::l2() << Log::tm() << "-check with " << wh << " warehouses succeeded\n";
    return true;
}

bool Schema::additionalPreparation(SQLHSTMT& hStmt) {

    for (auto stmt: DialectStrategy::getInstance()
                                     ->getAdditionalPreparationStatements()) {
        if (!DbcTools::executeServiceStatement(
                hStmt, stmt)) {
            Log::l2() << Log::tm() << "-failed\n";
            return false;
        }
    }
    Log::l2() << Log::tm() << "-succeeded\n";
    return true;
}
