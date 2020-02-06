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

#include "Transactions.h"

#include "DataSource.h"
#include "DbcTools.h"
#include "Log.h"
#include "Random.h"
#include "mz-config.h"

#include <cstdlib>
#include <cstring>
#include <string>

bool Transactions::prepare(Dialect* dialect, SQLHDBC& hDBC) {

    // NewOrder:
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noWarehouseSelect,
            dialect->getNoWarehouseSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noDistrictSelect,
            dialect->getNoDistrictSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noDistrictUpdate,
            dialect->getNoDistrictUpdate()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noCustomerSelect,
            dialect->getNoCustomerSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noItemSelect,
            dialect->getNoItemSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockSelects[0],
            dialect->getNoStockSelect01()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockSelects[1],
            dialect->getNoStockSelect02()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockSelects[2],
            dialect->getNoStockSelect03()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockSelects[3],
            dialect->getNoStockSelect04()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockSelects[4],
            dialect->getNoStockSelect05()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockSelects[5],
            dialect->getNoStockSelect06()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockSelects[6],
            dialect->getNoStockSelect07()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockSelects[7],
            dialect->getNoStockSelect08()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockSelects[8],
            dialect->getNoStockSelect09()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockSelects[9],
            dialect->getNoStockSelect10()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockUpdates[0],
            dialect->getNoStockUpdate01()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noStockUpdates[1],
            dialect->getNoStockUpdate02()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noOrderlineInsert,
            dialect->getNoOrderlineInsert()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noOrderInsert,
            dialect->getNoOrderInsert()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, noNewOrderInsert,
            dialect->getNoNewOrderInsert()))
        return false;

    // Payment:
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmWarehouseSelect,
            dialect->getPmWarehouseSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmWarehouseUpdate,
            dialect->getPmWarehouseUpdate()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmDistrictSelect,
            dialect->getPmDistrictSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmDistrictUpdate,
            dialect->getPmDistrictUpdate()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmCustomerSelect1,
            dialect->getPmCustomerSelect1()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmCustomerSelect2,
            dialect->getPmCustomerSelect2()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmCustomerSelect3,
            dialect->getPmCustomerSelect3()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmCustomerUpdate1,
            dialect->getPmCustomerUpdate1()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmCustomerSelect4,
            dialect->getPmCustomerSelect4()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmCustomerUpdate2,
            dialect->getPmCustomerUpdate2()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, pmHistoryInsert,
            dialect->getPmHistoryInsert()))
        return false;

    // OrderStatus:
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, osCustomerSelect1,
            dialect->getOsCustomerSelect1()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, osCustomerSelect2,
            dialect->getOsCustomerSelect2()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, osCustomerSelect3,
            dialect->getOsCustomerSelect3()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, osOrderSelect,
            dialect->getOsOrderSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, osOrderlineSelect,
            dialect->getOsOrderlineSelect()))
        return false;

    // Delivery
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, dlNewOrderSelect,
            dialect->getDlNewOrderSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, dlNewOrderDelete,
            dialect->getDlNewOrderDelete()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, dlOrderSelect,
            dialect->getDlOrderSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, dlOrderUpdate,
            dialect->getDlOrderUpdate()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, dlOrderlineUpdate,
            dialect->getDlOrderlineUpdate()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, dlOrderlineSelect,
            dialect->getDlOrderlineSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, dlCustomerUpdate,
            dialect->getDlCustomerUpdate()))
        return false;

    // StockLevel
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, slDistrictSelect,
            dialect->getSlDistrictSelect()))
        return false;
    if (!DbcTools::allocAndPrepareStmt(
            hDBC, slStockSelect,
            dialect->getSlStockSelect()))
        return false;

    return true;
}

bool Transactions::prepareStatements(Dialect* dialect, SQLHDBC& hDBC) {
    if (!prepare(dialect, hDBC)) {
        Log::l2() << Log::tm() << "-prepare statements failed\n";
        return false;
    }
    Log::l1() << Log::tm() << "-prepare statements succeeded\n";
    return true;
}

bool Transactions::executeNewOrder(Dialect* dialect, SQLHDBC& hDBC, mz::Config& cfg) {

    struct OrderLine {
        int olIId;
        int olSupplyWId;
        bool olIsRemote;
        int olQuantity;
    };

    // 2.4.1.1
    int wId = chRandom::uniformInt(1, warehouseCount);
    // 2.4.1.2
    int dId = chRandom::uniformInt(1, 10);
    int cId = chRandom::nonUniformInt(1023, 1, 3000, 867);
    // 2.4.1.3
    int olCount = chRandom::uniformInt(5, 15);
    // 2.4.1.4
    int randomRollback = chRandom::uniformInt(1, 100);
    // 2.4.1.5
    int allLocal = 1;
    OrderLine oLines[olCount];
    for (int i = 0; i < olCount; i++) {
        // 1.
        if (i == olCount - 1 && randomRollback == 1)
            oLines[i].olIId = 100001;
        else
            oLines[i].olIId = chRandom::nonUniformInt(8191, 1, 100000, 5867);
        // 2.
        if (chRandom::uniformInt(1, 100) == 1) {
            DataSource::getRemoteWId(wId, oLines[i].olSupplyWId);
            oLines[i].olIsRemote = true;
            allLocal = 0;
        } else {
            oLines[i].olSupplyWId = wId;
            oLines[i].olIsRemote = false;
        }
        // 3.
        oLines[i].olQuantity = chRandom::uniformInt(1, 10);
    }
    // 2.4.1.6
    SQL_TIMESTAMP_STRUCT oEntryD;
    DataSource::getCurrentTimestamp(oEntryD, cfg.order_entry_date_offset_millis(chRandom::rng));

    SQLLEN nIdicator = 0;
    SQLCHAR buf[1024] = {0};

    // BEGIN TRANSACTION
    DbcTools::resetStatement(noWarehouseSelect);
    DbcTools::bind(noWarehouseSelect, 1, wId);
    if (!DbcTools::executePreparedStatement(noWarehouseSelect)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    DbcTools::resetStatement(noDistrictSelect);
    DbcTools::bind(noDistrictSelect, 1, wId);
    DbcTools::bind(noDistrictSelect, 2, dId);
    if (!DbcTools::executePreparedStatement(noDistrictSelect)) {
        DbcTools::rollback(hDBC);
        return false;
    }
    int dNextOId = 0;
    if (!DbcTools::fetch(noDistrictSelect, buf, &nIdicator, 2, dNextOId)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    DbcTools::resetStatement(noDistrictUpdate);
    DbcTools::bind(noDistrictUpdate, 1, wId);
    DbcTools::bind(noDistrictUpdate, 2, dId);
    if (!DbcTools::executePreparedStatement(noDistrictUpdate)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    DbcTools::resetStatement(noCustomerSelect);
    DbcTools::bind(noCustomerSelect, 1, wId);
    DbcTools::bind(noCustomerSelect, 2, dId);
    DbcTools::bind(noCustomerSelect, 3, cId);
    if (!DbcTools::executePreparedStatement(noCustomerSelect)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    DbcTools::resetStatement(noOrderInsert);
    DbcTools::bind(noOrderInsert, 1, dNextOId);
    DbcTools::bind(noOrderInsert, 2, dId);
    DbcTools::bind(noOrderInsert, 3, wId);
    DbcTools::bind(noOrderInsert, 4, cId);
    DbcTools::bind(noOrderInsert, 5, oEntryD);
    DbcTools::bind(noOrderInsert, 6, olCount);
    DbcTools::bind(noOrderInsert, 7, allLocal);
    if (!DbcTools::executePreparedStatement(noOrderInsert)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    DbcTools::resetStatement(noNewOrderInsert);
    DbcTools::bind(noNewOrderInsert, 1, dNextOId);
    DbcTools::bind(noNewOrderInsert, 2, dId);
    DbcTools::bind(noNewOrderInsert, 3, wId);
    if (!DbcTools::executePreparedStatement(noNewOrderInsert)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    double iPrice;
    int sQuantity;
    std::string sDist;
    double tmp2;
    for (int i = 0; i < olCount; i++) {

        DbcTools::resetStatement(noItemSelect);
        DbcTools::bind(noItemSelect, 1, oLines[i].olIId);
        if (!DbcTools::executePreparedStatement(noItemSelect)) {
            DbcTools::rollback(hDBC);
            return false;
        }
        iPrice = 0;
        if (SQL_SUCCESS == SQLFetch(noItemSelect)) {
            if (SQL_SUCCESS ==
                SQLGetData(noItemSelect, 1, SQL_C_CHAR, buf, 1024, &nIdicator))
                iPrice = atof((char*) buf);
            else {
                DbcTools::rollback(hDBC);
                return false;
            }
        } else { // Expected Rollback
            if (DbcTools::rollback(hDBC))
                return true;
            return false;
        }

        DbcTools::resetStatement(noStockSelects[dId - 1]);
        DbcTools::bind(noStockSelects[dId - 1], 1, oLines[i].olIId);
        DbcTools::bind(noStockSelects[dId - 1], 2, oLines[i].olSupplyWId);
        if (!DbcTools::executePreparedStatement(noStockSelects[dId - 1])) {
            DbcTools::rollback(hDBC);
            return false;
        }
        sQuantity = 0;
        sDist = "";
        if (SQL_SUCCESS == SQLFetch(noStockSelects[dId - 1])) {
            if (SQL_SUCCESS == SQLGetData(noStockSelects[dId - 1], 1,
                                          SQL_C_CHAR, buf, 1024, &nIdicator)) {
                sQuantity = strtol((char*) buf, nullptr, 0);
            } else {
                DbcTools::rollback(hDBC);
                return false;
            }

            if (SQL_SUCCESS == SQLGetData(noStockSelects[dId - 1], 2,
                                          SQL_C_CHAR, buf, 1024, &nIdicator)) {
                sDist = std::string((char*) buf);
            } else {
                DbcTools::rollback(hDBC);
                return false;
            }
        } else {
            DbcTools::rollback(hDBC);
            return false;
        }

        DbcTools::resetStatement(
            noStockUpdates[(oLines[i].olIsRemote ? 1 : 0)]);
        DbcTools::bind(noStockUpdates[(oLines[i].olIsRemote ? 1 : 0)], 1,
                       oLines[i].olQuantity);
        int tmp1 = 0;
        if (oLines[i].olQuantity <= sQuantity - 10)
            tmp1 = sQuantity - oLines[i].olQuantity;
        else
            tmp1 = sQuantity - oLines[i].olQuantity + 91;
        DbcTools::bind(noStockUpdates[(oLines[i].olIsRemote ? 1 : 0)], 2, tmp1);
        DbcTools::bind(noStockUpdates[(oLines[i].olIsRemote ? 1 : 0)], 3,
                       oLines[i].olIId);
        DbcTools::bind(noStockUpdates[(oLines[i].olIsRemote ? 1 : 0)], 4,
                       oLines[i].olSupplyWId);
        if (!DbcTools::executePreparedStatement(
                noStockUpdates[(oLines[i].olIsRemote ? 1 : 0)])) {
            DbcTools::rollback(hDBC);
            return false;
        }

        DbcTools::resetStatement(noOrderlineInsert);
        DbcTools::bind(noOrderlineInsert, 1, dNextOId);
        DbcTools::bind(noOrderlineInsert, 2, dId);
        DbcTools::bind(noOrderlineInsert, 3, wId);
        tmp1 = i + 1;
        DbcTools::bind(noOrderlineInsert, 4, tmp1);
        DbcTools::bind(noOrderlineInsert, 5, (oLines[i].olIId));
        DbcTools::bind(noOrderlineInsert, 6, (oLines[i].olSupplyWId));
        DbcTools::bind(noOrderlineInsert, 7, (oLines[i].olQuantity));
        tmp2 = iPrice * oLines[i].olQuantity;
        DbcTools::bind(noOrderlineInsert, 8, tmp2);
        char buffer[24 + 1];
        strcpy(buffer, sDist.c_str());
        DbcTools::bind(noOrderlineInsert, 9, 24, buffer);
        if (!DbcTools::executePreparedStatement(noOrderlineInsert)) {
            DbcTools::rollback(hDBC);
            return false;
        }
    }

    // COMMIT
    if (DbcTools::commit(hDBC)) {
        return true;
    }
    DbcTools::rollback(hDBC);
    return false;
}

bool Transactions::executePayment(Dialect* dialect, SQLHDBC& hDBC, mz::Config& cfg) {

    // 2.5.1.1
    int wId = chRandom::uniformInt(1, warehouseCount);
    // 2.5.1.2
    int dId = chRandom::uniformInt(1, 10);

    int x = chRandom::uniformInt(1, 100);
    int cDId = 0;
    int cWId = 0;
    if (x <= 85) {
        cDId = dId;
        cWId = wId;
    } else {
        cDId = chRandom::uniformInt(1, 10);
        DataSource::getRemoteWId(wId, cWId);
    }

    int y = chRandom::uniformInt(1, 100);
    int cId = 0;
    std::string cLast;
    if (y <= 60) {
        DataSource::randomCLast(cLast);
    } else {
        cId = chRandom::nonUniformInt(1023, 1, 3000, 867);
    }

    // 2.5.1.3
    double hAmount = cfg.payment_amount_cents(chRandom::rng) / 100.0;

    // 2.5.1.4
    SQL_TIMESTAMP_STRUCT hDate;
    DataSource::getCurrentTimestamp(hDate,cfg.orderline_delivery_date_offset_millis(chRandom::rng));

    SQLLEN nIdicator = 0;
    SQLCHAR buf[1024] = {0};

    // BEGIN TRANSACTION
    DbcTools::resetStatement(pmWarehouseSelect);
    DbcTools::bind(pmWarehouseSelect, 1, wId);
    if (!DbcTools::executePreparedStatement(pmWarehouseSelect)) {
        DbcTools::rollback(hDBC);
        return false;
    }
    std::string wName;
    if (!DbcTools::fetch(pmWarehouseSelect, buf, &nIdicator, 1, wName)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    DbcTools::resetStatement(pmWarehouseUpdate);
    DbcTools::bind(pmWarehouseUpdate, 1, hAmount);
    DbcTools::bind(pmWarehouseUpdate, 2, wId);
    if (!DbcTools::executePreparedStatement(pmWarehouseUpdate)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    DbcTools::resetStatement(pmDistrictSelect);
    DbcTools::bind(pmDistrictSelect, 1, wId);
    DbcTools::bind(pmDistrictSelect, 2, dId);
    if (!DbcTools::executePreparedStatement(pmDistrictSelect)) {
        DbcTools::rollback(hDBC);
        return false;
    }
    std::string dName;
    if (!DbcTools::fetch(pmDistrictSelect, buf, &nIdicator, 1, dName)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    DbcTools::resetStatement(pmDistrictUpdate);
    DbcTools::bind(pmDistrictUpdate, 1, hAmount);
    DbcTools::bind(pmDistrictUpdate, 2, wId);
    DbcTools::bind(pmDistrictUpdate, 3, dId);
    if (!DbcTools::executePreparedStatement(pmDistrictUpdate)) {
        DbcTools::rollback(hDBC);
        return false;
    }
    std::string cCredit;
    if (y <= 60) { // Case 2
        DbcTools::resetStatement(pmCustomerSelect1);
        char buffer1[16 + 1];
        strcpy(buffer1, cLast.c_str());
        DbcTools::bind(pmCustomerSelect1, 1, 16, buffer1);
        DbcTools::bind(pmCustomerSelect1, 2, cDId);
        DbcTools::bind(pmCustomerSelect1, 3, cWId);
        if (!DbcTools::executePreparedStatement(pmCustomerSelect1)) {
            DbcTools::rollback(hDBC);
            return false;
        }
        int count = 0;
        if (!DbcTools::fetch(pmCustomerSelect1, buf, &nIdicator, 1, count)) {
            DbcTools::rollback(hDBC);
            return false;
        }

        DbcTools::resetStatement(pmCustomerSelect2);
        char buffer2[16 + 1];
        strcpy(buffer2, cLast.c_str());
        DbcTools::bind(pmCustomerSelect2, 1, 16, buffer2);
        DbcTools::bind(pmCustomerSelect2, 2, cDId);
        DbcTools::bind(pmCustomerSelect2, 3, cWId);
        if (!DbcTools::executePreparedStatement(pmCustomerSelect2)) {
            DbcTools::rollback(hDBC);
            return false;
        }
        cId = 0;
        cCredit = "";
        for (int i = 0; i < ((count + 1) / 2) - 1; i++) { // move cursor
            SQLFetch(pmCustomerSelect2);
        }
        if (SQL_SUCCESS == SQLFetch(pmCustomerSelect2)) {
            if (SQL_SUCCESS == SQLGetData(pmCustomerSelect2, 1, SQL_C_CHAR, buf,
                                          1024, &nIdicator))
                cId = strtol((char*) buf, nullptr, 0);
            else {
                DbcTools::rollback(hDBC);
                return false;
            }
            if (SQL_SUCCESS == SQLGetData(pmCustomerSelect2, 11, SQL_C_CHAR,
                                          buf, 1024, &nIdicator))
                cCredit = std::string((char*) buf);
            else {
                DbcTools::rollback(hDBC);
                return false;
            }
        } else {
            DbcTools::rollback(hDBC);
            return false;
        }
    } else { // Case 1
        DbcTools::resetStatement(pmCustomerSelect3);
        DbcTools::bind(pmCustomerSelect3, 1, cId);
        DbcTools::bind(pmCustomerSelect3, 2, cDId);
        DbcTools::bind(pmCustomerSelect3, 3, cWId);
        if (!DbcTools::executePreparedStatement(pmCustomerSelect3)) {
            DbcTools::rollback(hDBC);
            return false;
        }
        cCredit = "";
        if (!DbcTools::fetch(pmCustomerSelect3, buf, &nIdicator, 11, cCredit)) {
            DbcTools::rollback(hDBC);
            return false;
        }
    }

    DbcTools::resetStatement(pmCustomerUpdate1);
    DbcTools::bind(pmCustomerUpdate1, 1, hAmount);
    DbcTools::bind(pmCustomerUpdate1, 2, hAmount);
    DbcTools::bind(pmCustomerUpdate1, 3, cId);
    DbcTools::bind(pmCustomerUpdate1, 4, cDId);
    DbcTools::bind(pmCustomerUpdate1, 5, cWId);
    if (!DbcTools::executePreparedStatement(pmCustomerUpdate1)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    if (cCredit == "BC") {
        DbcTools::resetStatement(pmCustomerSelect4);
        DbcTools::bind(pmCustomerSelect4, 1, cId);
        DbcTools::bind(pmCustomerSelect4, 2, cDId);
        DbcTools::bind(pmCustomerSelect4, 3, cWId);
        if (!DbcTools::executePreparedStatement(pmCustomerSelect4)) {
            DbcTools::rollback(hDBC);
            return false;
        }
        std::string cData;
        if (!DbcTools::fetch(pmCustomerSelect4, buf, &nIdicator, 1, cData)) {
            DbcTools::rollback(hDBC);
            return false;
        }
        cData = std::to_string(cId) + "," + std::to_string(cDId) + "," +
                std::to_string(cWId) + "," + std::to_string(dId) + "," +
                std::to_string(wId) + "," + std::to_string(hAmount) + "," +
                cData;
        if (cData.length() > 500)
            cData = cData.substr(0, 500);

        DbcTools::resetStatement(pmCustomerUpdate2);
        char buffer3[500 + 1];
        strcpy(buffer3, cData.c_str());
        DbcTools::bind(pmCustomerUpdate2, 1, 500, buffer3);
        DbcTools::bind(pmCustomerUpdate2, 2, cId);
        DbcTools::bind(pmCustomerUpdate2, 3, cDId);
        DbcTools::bind(pmCustomerUpdate2, 4, cWId);
        if (!DbcTools::executePreparedStatement(pmCustomerUpdate2)) {
            DbcTools::rollback(hDBC);
            return false;
        }
    }

    std::string hData = wName + "    " + dName;

    DbcTools::resetStatement(pmHistoryInsert);
    DbcTools::bind(pmHistoryInsert, 1, cId);
    DbcTools::bind(pmHistoryInsert, 2, cDId);
    DbcTools::bind(pmHistoryInsert, 3, cWId);
    DbcTools::bind(pmHistoryInsert, 4, dId);
    DbcTools::bind(pmHistoryInsert, 5, wId);
    DbcTools::bind(pmHistoryInsert, 6, hDate);
    DbcTools::bind(pmHistoryInsert, 7, hAmount);
    char buffer4[24 + 1];
    strcpy(buffer4, hData.c_str());
    DbcTools::bind(pmHistoryInsert, 8, 24, buffer4);
    if (!DbcTools::executePreparedStatement(pmHistoryInsert)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    // COMMIT
    if (DbcTools::commit(hDBC)) {
        return true;
    }
    DbcTools::rollback(hDBC);
    return false;
}

bool Transactions::executeOrderStatus(Dialect* dialect, SQLHDBC& hDBC) {

    // 2.6.1.1
    int wId = chRandom::uniformInt(1, warehouseCount);
    // 2.6.1.2
    int dId = chRandom::uniformInt(1, 10);
    int y = chRandom::uniformInt(1, 100);
    int cId = 0;
    std::string cLast;
    if (y <= 60) {
        DataSource::randomCLast(cLast);
    } else {
        cId = chRandom::nonUniformInt(1023, 1, 3000, 867);
    }

    SQLLEN nIdicator = 0;
    SQLCHAR buf[1024] = {0};

    // BEGIN TRANSACTION
    if (y <= 60) { // Case 2
        DbcTools::resetStatement(osCustomerSelect1);
        char buffer1[16 + 1];
        strcpy(buffer1, cLast.c_str());
        DbcTools::bind(osCustomerSelect1, 1, 16, buffer1);
        DbcTools::bind(osCustomerSelect1, 2, dId);
        DbcTools::bind(osCustomerSelect1, 3, wId);
        if (!DbcTools::executePreparedStatement(osCustomerSelect1)) {
            DbcTools::rollback(hDBC);
            return false;
        }
        int count = 0;
        if (!DbcTools::fetch(osCustomerSelect1, buf, &nIdicator, 1, count)) {
            DbcTools::rollback(hDBC);
            return false;
        }

        DbcTools::resetStatement(osCustomerSelect2);
        char buffer2[16 + 1];
        strcpy(buffer2, cLast.c_str());
        DbcTools::bind(osCustomerSelect2, 1, 16, buffer2);
        DbcTools::bind(osCustomerSelect2, 2, dId);
        DbcTools::bind(osCustomerSelect2, 3, wId);
        if (!DbcTools::executePreparedStatement(osCustomerSelect2)) {
            DbcTools::rollback(hDBC);
            return false;
        }

        for (int i = 0; i < ((count + 1) / 2) - 1; i++) { // move cursor
            SQLFetch(osCustomerSelect2);
        }
        if (!DbcTools::fetch(osCustomerSelect2, buf, &nIdicator, 1, cId)) {
            DbcTools::rollback(hDBC);
            return false;
        }
    } else { // Case 1
        DbcTools::resetStatement(osCustomerSelect3);
        DbcTools::bind(osCustomerSelect3, 1, cId);
        DbcTools::bind(osCustomerSelect3, 2, dId);
        DbcTools::bind(osCustomerSelect3, 3, wId);
        if (!DbcTools::executePreparedStatement(osCustomerSelect3)) {
            DbcTools::rollback(hDBC);
            return false;
        }
    }

    DbcTools::resetStatement(osOrderSelect);
    DbcTools::bind(osOrderSelect, 1, wId);
    DbcTools::bind(osOrderSelect, 2, dId);
    DbcTools::bind(osOrderSelect, 3, cId);
    DbcTools::bind(osOrderSelect, 4, wId);
    DbcTools::bind(osOrderSelect, 5, dId);
    DbcTools::bind(osOrderSelect, 6, cId);
    if (!DbcTools::executePreparedStatement(osOrderSelect)) {
        DbcTools::rollback(hDBC);
        return false;
    }
    int oId = 0;
    if (!DbcTools::fetch(osOrderSelect, buf, &nIdicator, 1, oId)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    DbcTools::resetStatement(osOrderlineSelect);
    DbcTools::bind(osOrderlineSelect, 1, wId);
    DbcTools::bind(osOrderlineSelect, 2, dId);
    DbcTools::bind(osOrderlineSelect, 3, oId);
    if (!DbcTools::executePreparedStatement(osOrderlineSelect)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    // COMMIT
    if (DbcTools::commit(hDBC)) {
        return true;
    }
    DbcTools::rollback(hDBC);
    return false;
}

bool Transactions::executeDelivery(Dialect* dialect, SQLHDBC& hDBC, mz::Config& cfg) {

    // 2.7.1.1
    int wId = chRandom::uniformInt(1, warehouseCount);
    // 2.7.1.2
    int oCarrierId = chRandom::uniformInt(1, 10);
    // 2.7.1.3
    SQL_TIMESTAMP_STRUCT olDeliveryD;
    DataSource::getCurrentTimestamp(olDeliveryD, cfg.orderline_delivery_date_offset_millis(chRandom::rng));

    SQLLEN nIdicator = 0;
    SQLCHAR buf[1024] = {0};

    // BEGIN TRANSACTION
    int noOId;
    int oCId;
    double olAmount;
    for (int dId = 1; dId <= 10; dId++) {

        DbcTools::resetStatement(dlNewOrderSelect);
        DbcTools::bind(dlNewOrderSelect, 1, wId);
        DbcTools::bind(dlNewOrderSelect, 2, dId);
        DbcTools::bind(dlNewOrderSelect, 3, wId);
        DbcTools::bind(dlNewOrderSelect, 4, dId);
        if (!DbcTools::executePreparedStatement(dlNewOrderSelect)) {
            DbcTools::rollback(hDBC);
            return false;
        }
        noOId = 0;
        if (SQL_SUCCESS == SQLFetch(dlNewOrderSelect)) {
            if (SQL_SUCCESS == SQLGetData(dlNewOrderSelect, 1, SQL_C_CHAR, buf,
                                          1024, &nIdicator))
                noOId = strtol((char*) buf, nullptr, 0);
            else {
                DbcTools::rollback(hDBC);
                return false;
            }
        } else // If no matching row is found, then the delivery of an order for
               // this district is skipped.
            continue;

        DbcTools::resetStatement(dlNewOrderDelete);
        DbcTools::bind(dlNewOrderDelete, 1, wId);
        DbcTools::bind(dlNewOrderDelete, 2, dId);
        DbcTools::bind(dlNewOrderDelete, 3, noOId);
        if (!DbcTools::executePreparedStatement(dlNewOrderDelete)) {
            DbcTools::rollback(hDBC);
            return false;
        }

        DbcTools::resetStatement(dlOrderSelect);
        DbcTools::bind(dlOrderSelect, 1, wId);
        DbcTools::bind(dlOrderSelect, 2, dId);
        DbcTools::bind(dlOrderSelect, 3, noOId);
        if (!DbcTools::executePreparedStatement(dlOrderSelect)) {
            DbcTools::rollback(hDBC);
            return false;
        }
        oCId = 0;
        if (!DbcTools::fetch(dlOrderSelect, buf, &nIdicator, 1, oCId)) {
            DbcTools::rollback(hDBC);
            return false;
        }

        DbcTools::resetStatement(dlOrderUpdate);
        DbcTools::bind(dlOrderUpdate, 1, oCarrierId);
        DbcTools::bind(dlOrderUpdate, 2, wId);
        DbcTools::bind(dlOrderUpdate, 3, dId);
        DbcTools::bind(dlOrderUpdate, 4, noOId);
        if (!DbcTools::executePreparedStatement(dlOrderUpdate)) {
            DbcTools::rollback(hDBC);
            return false;
        }

        DbcTools::resetStatement(dlOrderlineUpdate);
        DbcTools::bind(dlOrderlineUpdate, 1, olDeliveryD);
        DbcTools::bind(dlOrderlineUpdate, 2, wId);
        DbcTools::bind(dlOrderlineUpdate, 3, dId);
        DbcTools::bind(dlOrderlineUpdate, 4, noOId);
        if (!DbcTools::executePreparedStatement(dlOrderlineUpdate)) {
            DbcTools::rollback(hDBC);
            return false;
        }

        DbcTools::resetStatement(dlOrderlineSelect);
        DbcTools::bind(dlOrderlineSelect, 1, wId);
        DbcTools::bind(dlOrderlineSelect, 2, dId);
        DbcTools::bind(dlOrderlineSelect, 3, noOId);
        if (!DbcTools::executePreparedStatement(dlOrderlineSelect)) {
            DbcTools::rollback(hDBC);
            return false;
        }
        olAmount = 0;
        if (!DbcTools::fetch(dlOrderlineSelect, buf, &nIdicator, 1, olAmount)) {
            DbcTools::rollback(hDBC);
            return false;
        }

        DbcTools::resetStatement(dlCustomerUpdate);
        DbcTools::bind(dlCustomerUpdate, 1, olAmount);
        DbcTools::bind(dlCustomerUpdate, 2, oCId);
        DbcTools::bind(dlCustomerUpdate, 3, dId);
        DbcTools::bind(dlCustomerUpdate, 4, wId);
        if (!DbcTools::executePreparedStatement(dlCustomerUpdate)) {
            DbcTools::rollback(hDBC);
            return false;
        }

        // COMMIT
        if (!DbcTools::commit(hDBC)) {
            DbcTools::rollback(hDBC);
            return false;
        }
    }
    return true;
}

bool Transactions::executeStockLevel(Dialect* dialect, SQLHDBC& hDBC) {

    // 2.8.1.1
    int wId = chRandom::uniformInt(1, warehouseCount);
    int dId = chRandom::uniformInt(1, 10);
    // 2.8.1.2
    int threshold = chRandom::uniformInt(10, 20);

    SQLLEN nIdicator = 0;
    SQLCHAR buf[1024] = {0};

    // BEGIN TRANSACTION
    DbcTools::resetStatement(slDistrictSelect);
    DbcTools::bind(slDistrictSelect, 1, wId);
    DbcTools::bind(slDistrictSelect, 2, dId);
    if (!DbcTools::executePreparedStatement(slDistrictSelect)) {
        DbcTools::rollback(hDBC);
        return false;
    }
    int dNextOId = 0;
    if (!DbcTools::fetch(slDistrictSelect, buf, &nIdicator, 1, dNextOId)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    DbcTools::resetStatement(slStockSelect);
    DbcTools::bind(slStockSelect, 1, wId);
    DbcTools::bind(slStockSelect, 2, dId);
    DbcTools::bind(slStockSelect, 3, dNextOId);
    int tmp = dNextOId - 20;
    DbcTools::bind(slStockSelect, 4, tmp);
    DbcTools::bind(slStockSelect, 5, wId);
    DbcTools::bind(slStockSelect, 6, threshold);
    if (!DbcTools::executePreparedStatement(slStockSelect)) {
        DbcTools::rollback(hDBC);
        return false;
    }

    // COMMIT
    if (DbcTools::commit(hDBC)) {
        return true;
    }
    DbcTools::rollback(hDBC);
    return false;
}
