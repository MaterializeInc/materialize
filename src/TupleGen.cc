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

#include "TupleGen.h"

#include "Defines.h"

#include <err.h>

using namespace std;

ofstream TupleGen::warehouseStream;
ofstream TupleGen::districtStream;
ofstream TupleGen::customerStream;
ofstream TupleGen::historyStream;
ofstream TupleGen::neworderStream;
ofstream TupleGen::orderStream;
ofstream TupleGen::orderlineStream;
ofstream TupleGen::itemStream;
ofstream TupleGen::stockStream;
ofstream TupleGen::nationStream;
ofstream TupleGen::supplierStream;
ofstream TupleGen::regionStream;

void ofopen(ofstream& f, std::string path) {
    f.open(path);
    if (f.fail()) {
        err(1, "opening %s", path.c_str());
    }
}

void TupleGen::openOutputFiles(std::string outDir) {
    ofopen(warehouseStream, outDir + "/warehouse.tbl");
    ofopen(districtStream, outDir + "/district.tbl");
    ofopen(customerStream, outDir + "/customer.tbl");
    ofopen(historyStream, outDir + "/history.tbl");
    ofopen(neworderStream, outDir + "/neworder.tbl");
    ofopen(orderStream, outDir + "/order.tbl");
    ofopen(orderlineStream, outDir + "/orderline.tbl");
    ofopen(itemStream, outDir + "/item.tbl");
    ofopen(stockStream, outDir + "/stock.tbl");
    ofopen(nationStream, outDir + "/nation.tbl");
    ofopen(supplierStream, outDir + "/supplier.tbl");
    ofopen(regionStream, outDir + "/region.tbl");
}

void TupleGen::closeOutputFiles() {
    warehouseStream.close();
    districtStream.close();
    customerStream.close();
    historyStream.close();
    neworderStream.close();
    orderStream.close();
    orderlineStream.close();
    itemStream.close();
    stockStream.close();
    nationStream.close();
    supplierStream.close();
    regionStream.close();
}

void TupleGen::genWarehouse(int& wId) {
    warehouseStream << wId << csvDelim;                        // W_ID
    DataSource::addAlphanumeric64(6, 10, warehouseStream, 1);  // W_NAME
    DataSource::addAlphanumeric64(10, 20, warehouseStream, 1); // W_STREET_1
    DataSource::addAlphanumeric64(10, 20, warehouseStream, 1); // W_STREET_2
    DataSource::addAlphanumeric64(10, 20, warehouseStream, 1); // W_CITY
    DataSource::addAlphanumeric62(2, warehouseStream, 1);      // W_STATE
    DataSource::addWDCZip(warehouseStream, 1);                 // W_ZIP
    DataSource::addDouble(0.0, 0.2, 4, warehouseStream, 1);    // W_TAX
    warehouseStream << "300000.00";                            // W_YTD
    warehouseStream << endl;
}

void TupleGen::genDistrict(int& dId, int& wId) {
    districtStream << dId << csvDelim;                        // D_ID
    districtStream << wId << csvDelim;                        // D_W_ID
    DataSource::addAlphanumeric64(6, 10, districtStream, 1);  // D_NAME
    DataSource::addAlphanumeric64(10, 20, districtStream, 1); // D_STREET_1
    DataSource::addAlphanumeric64(10, 20, districtStream, 1); // D_STREET_2
    DataSource::addAlphanumeric64(10, 20, districtStream, 1); // D_CITY
    DataSource::addAlphanumeric62(2, districtStream, 1);      // D_STATE
    DataSource::addWDCZip(districtStream, 1);                 // D_ZIP
    DataSource::addDouble(0.0, 0.2, 4, districtStream, 1);    // D_TAX
    districtStream << "30000.00" << csvDelim;                 // D_YTD
    districtStream << "3001";                                 // D_NEXT_O_ID
    districtStream << endl;
}

void TupleGen::genCustomer(int& cId, int& dId, int& wId, string& customerTime) {
    string cLast = "";
    if (cId <= 1000)
        DataSource::genCLast(cId - 1, cLast);
    else
        DataSource::randomCLast(cLast);

    string cState = DataSource::randomAlphanumeric62(2);

    customerStream << cId << csvDelim;                        // C_ID
    customerStream << dId << csvDelim;                        // C_D_ID
    customerStream << wId << csvDelim;                        // C_W_ID
    DataSource::addAlphanumeric64(8, 16, customerStream, 1);  // C_FIRST
    customerStream << "OE" << csvDelim;                       // C_MIDDLE
    customerStream << cLast << csvDelim;                      // C_LAST
    DataSource::addAlphanumeric64(10, 20, customerStream, 1); // C_STREET_1
    DataSource::addAlphanumeric64(10, 20, customerStream, 1); // C_STREET_2
    DataSource::addAlphanumeric64(10, 20, customerStream, 1); // C_CITY
    customerStream << cState << csvDelim;                     // C_STATE
    DataSource::addWDCZip(customerStream, 1);                 // C_ZIP
    DataSource::addNumeric(16, customerStream, 1);            // C_PHONE
    customerStream << customerTime
                   << csvDelim; // C_SINCE - date/time given by the os when
                                // the CUSTOMER table was populated
    customerStream << (DataSource::randomTrue(0.1) ? "BC" : "GC")
                   << csvDelim;                            // C_CREDIT
    customerStream << "50000.00" << csvDelim;              // C_CREDIT_LIM
    DataSource::addDouble(0.0, 0.5, 4, customerStream, 1); // C_DISCOUNT
    customerStream << "-10.00" << csvDelim;                // C_BALANCE
    customerStream << "10.00" << csvDelim;                 // C_YTD_PAYMENT
    customerStream << "1" << csvDelim;                     // C_PAYMENT_CNT
    customerStream << "0" << csvDelim;                     // C_DELIVERY_CNT
    DataSource::addAlphanumeric64(300, 500, customerStream, 1); // C_DATA
    customerStream << (int)(cState.c_str())[0];                 // C_N_NATIONKEY
    customerStream << endl;
}

void TupleGen::genHistory(int& cId, int& dId, int& wId) {
    historyStream << cId << csvDelim; // H_C_ID
    historyStream << dId << csvDelim; // H_C_D_ID
    historyStream << wId << csvDelim; // H_C_W_ID
    historyStream << dId << csvDelim; // H_D_ID
    historyStream << wId << csvDelim; // H_W_ID
    historyStream << DataSource::getCurrentTimeString()
                  << csvDelim;            // H_DATE - current date and time
    historyStream << "10.00" << csvDelim; // H_AMOUNT
    DataSource::addAlphanumeric64(12, 24, historyStream, 0); // H_DATA
    historyStream << endl;
}

void TupleGen::genNeworder(int& oId, int& dId, int& wId) {
    neworderStream << oId << csvDelim; // NO_O_ID
    neworderStream << dId << csvDelim; // NO_D_ID
    neworderStream << wId;             // NO_W_ID
    neworderStream << endl;
}

void TupleGen::genOrder(int& oId, int& dId, int& wId, int& cId, int& olCount,
                        string& orderTime) {
    orderStream << oId << csvDelim;       // O_ID
    orderStream << dId << csvDelim;       // O_D_ID
    orderStream << wId << csvDelim;       // O_W_ID
    orderStream << cId << csvDelim;       // O_C_ID
    orderStream << orderTime << csvDelim; // O_ENTRY_D - current date/ time
                                          // given by the os
    if (oId <= 2100) // O_CARRIER_ID - random within [1 .. 10] if O_ID <= 2100,
                     // null otherwise
        DataSource::addInt(1, 10, orderStream, 1);
    else
        orderStream << "" << csvDelim;
    orderStream << olCount << csvDelim; // O_OL_CNT
    orderStream << "1";                 // O_ALL_LOCAL
    orderStream << endl;
}

void TupleGen::genOrderline(int& oId, int& dId, int& wId, int& olNumber,
                            string& orderTime) {
    orderlineStream << oId << csvDelim;                // OL_O_ID
    orderlineStream << dId << csvDelim;                // OL_D_ID
    orderlineStream << wId << csvDelim;                // OL_W_ID
    orderlineStream << olNumber << csvDelim;           // OL_NUMBER
    DataSource::addInt(1, 100000, orderlineStream, 1); // OL_I_ID
    orderlineStream << wId << csvDelim;                // OL_SUPPLY_W_ID
    orderlineStream << (oId <= 2100 ? orderTime : "")
                    << csvDelim;        // OL_DELIVERY_D = O_ENTRY_D if
                                        // OL_O_ID <= 2100, null otherwise
    orderlineStream << "5" << csvDelim; // OL_QUANTITY
    if (oId <= 2100) // OL_AMOUNT = 0.00 if OL_O_ID <= 2100, random within
                     // [0.01..9999.99] otherwise
        orderlineStream << "0.00" << csvDelim;
    else
        DataSource::addDouble(0.01, 9999.99, 2, orderlineStream, 1);
    DataSource::addAlphanumeric64(24, orderlineStream, 0); // OL_DIST_INFO
    orderlineStream << endl;
}

void TupleGen::genItem(int& iId) {
    itemStream << iId << csvDelim;                        // I_ID
    DataSource::addInt(1, 10000, itemStream, 1);          // I_IM_ID
    DataSource::addAlphanumeric64(14, 24, itemStream, 1); // I_NAME
    DataSource::addDouble(1.0, 100.0, 2, itemStream, 1);  // I_PRICE
    if (DataSource::randomTrue(0.1))                      // I_DATA
        DataSource::addAlphanumeric64Original(26, 50, itemStream, 0);
    else
        DataSource::addAlphanumeric64(26, 50, itemStream, 0);
    itemStream << endl;
}

void TupleGen::genStock(int& iId, int& wId) {
    stockStream << iId << csvDelim;                    // S_I_ID
    stockStream << wId << csvDelim;                    // S_W_ID
    DataSource::addInt(10, 100, stockStream, 1);       // S_QUANTITY
    DataSource::addAlphanumeric64(24, stockStream, 1); // S_DIST_01
    DataSource::addAlphanumeric64(24, stockStream, 1); // S_DIST_02
    DataSource::addAlphanumeric64(24, stockStream, 1); // S_DIST_03
    DataSource::addAlphanumeric64(24, stockStream, 1); // S_DIST_04
    DataSource::addAlphanumeric64(24, stockStream, 1); // S_DIST_05
    DataSource::addAlphanumeric64(24, stockStream, 1); // S_DIST_06
    DataSource::addAlphanumeric64(24, stockStream, 1); // S_DIST_07
    DataSource::addAlphanumeric64(24, stockStream, 1); // S_DIST_08
    DataSource::addAlphanumeric64(24, stockStream, 1); // S_DIST_09
    DataSource::addAlphanumeric64(24, stockStream, 1); // S_DIST_10
    stockStream << "0" << csvDelim;                    // S_YTD
    stockStream << "0" << csvDelim;                    // S_ORDER_CNT
    stockStream << "0" << csvDelim;                    // S_REMOTE_CNT
    if (DataSource::randomTrue(0.1))                   // S_DATA
        DataSource::addAlphanumeric64Original(26, 50, stockStream, 1);
    else
        DataSource::addAlphanumeric64(26, 50, stockStream, 1);
    stockStream << ((iId * wId) %
                    10000); // S_SU_SUPPKEY - no TPC-C/CH-benCHmark spec
    stockStream << endl;
}

void TupleGen::genNation(Nation n) {
    nationStream << n.id << csvDelim;                    // N_NATIONKEY
    nationStream << n.name << csvDelim;                  // N_NAME
    nationStream << n.rId << csvDelim;                   // N_REGIONKEY
    DataSource::addTextString(31, 114, nationStream, 0); // N_COMMENT
    nationStream << endl;
}

void TupleGen::genSupplier(int& suId) {
    supplierStream << suId << csvDelim; // SU_SUPPKEY
    supplierStream << "Supplier#" << DataSource::strLeadingZero(suId, 9)
                   << csvDelim;                               // SU_NAME
    DataSource::addAlphanumeric64(10, 40, supplierStream, 1); // SU_ADDRESS
    DataSource::addNId(supplierStream, 1);                    // SU_NATIONKEY
    DataSource::addSuPhone(suId, supplierStream, 1);          // SU_PHONE
    DataSource::addDouble(-999.99, 9999.99, 2, supplierStream, 1); // SU_ACCTBAL
    if ((suId + 7) % 1893 == 0)                                    // no spec
                                // SU_COMMENT: 5 rows "Customer%Recommends" + 5
                                // rows "Customer%Complaints"
        DataSource::addTextStringCustomer(25, 100, "Complaints", supplierStream,
                                          0);
    else if ((suId + 13) % 1983 == 0) // no spec
        DataSource::addTextStringCustomer(25, 100, "Recommends", supplierStream,
                                          0);
    else
        DataSource::addTextString(25, 100, supplierStream, 0);
    supplierStream << endl;
}

void TupleGen::genRegion(int& rId, const char* rName) {
    regionStream << rId << csvDelim;                     // R_REGIONKEY
    regionStream << rName << csvDelim;                   // R_NAME
    DataSource::addTextString(31, 115, regionStream, 0); // R_COMMENT
    regionStream << endl;
}
