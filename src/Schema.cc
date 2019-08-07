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
#include <stdlib.h>

using namespace std;

bool Schema::check(SQLHSTMT& hStmt, const char* query, int& cnt) {

    SQLLEN nIdicator = 0;
    SQLCHAR buf[128] = {0};

    if (!DbcTools::executeServiceStatement(hStmt, query)) {
        Log::l2() << Log::tm() << "-determine count failed\n";
        return 0;
    }
    if (!DbcTools::fetch(hStmt, buf, &nIdicator, 1, cnt)) {
        Log::l2() << Log::tm() << "-determine count failed\n";
        return 0;
    }
    if (cnt == 0) {
        Log::l2() << Log::tm() << "-determine count failed\n";
        return 0;
    }
    return 1;
}

bool Schema::createSchema(SQLHSTMT& hStmt) {

    for (unsigned int i = 0; i < DialectStrategy::getInstance()
                                     ->getDropExistingSchemaStatements()
                                     .size();
         i++) {
        DbcTools::executeServiceStatement(
            hStmt,
            DialectStrategy::getInstance()
                ->getDropExistingSchemaStatements()[i],
            0);
    }

    for (unsigned int i = 0;
         i < DialectStrategy::getInstance()->getCreateSchemaStatements().size();
         i++) {
        if (!DbcTools::executeServiceStatement(
                hStmt, DialectStrategy::getInstance()
                           ->getCreateSchemaStatements()[i])) {
            Log::l2() << Log::tm() << "-failed\n";
            return 0;
        }
    }

    Log::l2() << Log::tm() << "-succeeded\n";
    return 1;
}

bool Schema::importCSV(SQLHSTMT& hStmt, const std::string& genDir) {

    if (DialectStrategy::getInstance()->getImportPrefix().size() !=
        DialectStrategy::getInstance()->getImportSuffix().size()) {
        Log::l2()
            << Log::tm()
            << "-failed (different size of ImportPrefix and ImportSuffix vector in dialect class)\n";
        return 0;
    }

    for (unsigned int i = 0;
         i < DialectStrategy::getInstance()->getImportPrefix().size(); i++) {
        if (!DbcTools::executeServiceStatement(
                hStmt,
                string(DialectStrategy::getInstance()->getImportPrefix()[i] +
                       genDir +
                       DialectStrategy::getInstance()->getImportSuffix()[i])
                    .c_str())) {
            Log::l2() << Log::tm() << "-failed\n";
            return 0;
        }
    }

    Log::l2() << Log::tm() << "-succeeded\n";
    return 1;
}

bool Schema::check(SQLHSTMT& hStmt) {

    int wh = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountWarehouse(),
               wh))
        return 0;

    int ds = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountDistrict(),
               ds))
        return 0;
    if (ds != 10 * wh) {
        Log::l2() << Log::tm() << "-check failed (#DISTRICT: " << ds << ")\n";
        return 0;
    }

    int cs = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountCustomer(),
               cs))
        return 0;
    if (cs != 30000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#CUSTOMER: " << cs << ")\n";
        return 0;
    }

    int od = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountOrder(),
               od))
        return 0;
    if (od != 30000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#ORDER: " << od << ")\n";
        return 0;
    }

    int ol = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountOrderline(),
               ol))
        return 0;
    if (ol != 300000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#ORDERLINE: " << ol << ")\n";
        return 0;
    }

    int no = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountNeworder(),
               no))
        return 0;
    if (no != 9000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#NEWORDER: " << no << ")\n";
        return 0;
    }

    int hs = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountHistory(),
               hs))
        return 0;
    if (hs != 30000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#HISTORY: " << hs << ")\n";
        return 0;
    }

    int st = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountStock(),
               st))
        return 0;
    if (st != 100000 * wh) {
        Log::l2() << Log::tm() << "-check failed (#STOCK: " << st << ")\n";
        return 0;
    }

    int it = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountItem(), it))
        return 0;
    if (it != 100000) {
        Log::l2() << Log::tm() << "-check failed (#ITEM: " << it << ")\n";
        return 0;
    }

    int sp = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountSupplier(),
               sp))
        return 0;
    if (sp != 10000) {
        Log::l2() << Log::tm() << "-check failed (#SUPPLIER: " << sp << ")\n";
        return 0;
    }

    int na = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountNation(),
               na))
        return 0;
    if (na != 62) {
        Log::l2() << Log::tm() << "-check failed (#NATION: " << na << ")\n";
        return 0;
    }

    int rg = 0;
    if (!check(hStmt, DialectStrategy::getInstance()->getSelectCountRegion(),
               rg))
        return 0;
    if (rg != 5) {
        Log::l2() << Log::tm() << "-check failed (#REGION: " << rg << ")\n";
        return 0;
        ;
    }

    Log::l2() << Log::tm() << "-check with " << wh << " warehouses succeeded\n";
    return 1;
}

bool Schema::additionalPreparation(SQLHSTMT& hStmt) {

    for (unsigned int i = 0; i < DialectStrategy::getInstance()
                                     ->getAdditionalPreparationStatements()
                                     .size();
         i++) {
        if (!DbcTools::executeServiceStatement(
                hStmt, DialectStrategy::getInstance()
                           ->getAdditionalPreparationStatements()[i])) {
            Log::l2() << Log::tm() << "-failed\n";
            return 0;
        }
    }
    Log::l2() << Log::tm() << "-succeeded\n";
    return 1;
}
