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

#ifndef TUPLEGEN_H
#define TUPLEGEN_H

#include "DataSource.h"

#include <fstream>
#include <string>
#include "mz-config.h"

class TupleGen {

  private:
    static std::ofstream warehouseStream;
    static std::ofstream districtStream;
    static std::ofstream customerStream;
    static std::ofstream historyStream;
    static std::ofstream neworderStream;
    static std::ofstream orderStream;
    static std::ofstream orderlineStream;
    static std::ofstream itemStream;
    static std::ofstream stockStream;
    static std::ofstream nationStream;
    static std::ofstream supplierStream;
    static std::ofstream regionStream;

  public:
    static void openOutputFiles(const std::string& outDir);
    static void closeOutputFiles();
    static void genWarehouse(int& wId);
    static void genDistrict(int& dId, int& wId);
    static void genCustomer(int& cId, int& dId, int& wId,
                            std::string& customerTime);
    static void genHistory(int& cId, int& dId, int& wId, mz::Config& cfg);
    static void genNeworder(int& oId, int& dId, int& wId);
    static void genOrder(int& oId, int& dId, int& wId, int& cId, int& olCount,
                         std::string& orderTime);
    static void genOrderline(int& oId, int& dId, int& wId, int& olNumber,
                             std::string& orderTime);
    static void genItem(int& iId, mz::Config& cfg);
    static void genStock(int& iId, int& wId);
    static void genNation(const Nation& n);
    static void genSupplier(int& suId);
    static void genRegion(int& rId, const char* rName);
};

#endif
