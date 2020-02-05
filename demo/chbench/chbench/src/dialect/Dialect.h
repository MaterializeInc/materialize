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

#ifndef DIALECTBASIS_H
#define DIALECTBASIS_H

#include <vector>

class Dialect {

  public:
    virtual ~Dialect(){};

    // Strings to create initial database
    virtual std::vector<const char*>& getDropExistingSchemaStatements() = 0;
    virtual std::vector<const char*>& getCreateSchemaStatements() = 0;
    virtual std::vector<const char*>& getImportPrefix() = 0;
    virtual std::vector<const char*>& getImportSuffix() = 0;
    virtual std::vector<const char*>& getAdditionalPreparationStatements() = 0;

    // 22 adjusted TPC-H OLAP query strings
    virtual std::vector<const char*>& getTpchQueryStrings() = 0;

    // Strings for database check
    virtual const char* getSelectCountWarehouse() = 0;
    virtual const char* getSelectCountDistrict() = 0;
    virtual const char* getSelectCountCustomer() = 0;
    virtual const char* getSelectCountOrder() = 0;
    virtual const char* getSelectCountOrderline() = 0;
    virtual const char* getSelectCountNeworder() = 0;
    virtual const char* getSelectCountHistory() = 0;
    virtual const char* getSelectCountStock() = 0;
    virtual const char* getSelectCountItem() = 0;
    virtual const char* getSelectCountSupplier() = 0;
    virtual const char* getSelectCountNation() = 0;
    virtual const char* getSelectCountRegion() = 0;

    // TPC-C transaction strings
    // NewOrder:
    virtual const char* getNoWarehouseSelect() = 0;
    virtual const char* getNoDistrictSelect() = 0;
    virtual const char* getNoDistrictUpdate() = 0;
    virtual const char* getNoCustomerSelect() = 0;
    virtual const char* getNoOrderInsert() = 0;
    virtual const char* getNoNewOrderInsert() = 0;
    virtual const char* getNoItemSelect() = 0;
    virtual const char* getNoStockSelect01() = 0;
    virtual const char* getNoStockSelect02() = 0;
    virtual const char* getNoStockSelect03() = 0;
    virtual const char* getNoStockSelect04() = 0;
    virtual const char* getNoStockSelect05() = 0;
    virtual const char* getNoStockSelect06() = 0;
    virtual const char* getNoStockSelect07() = 0;
    virtual const char* getNoStockSelect08() = 0;
    virtual const char* getNoStockSelect09() = 0;
    virtual const char* getNoStockSelect10() = 0;
    virtual const char* getNoStockUpdate01() = 0;
    virtual const char* getNoStockUpdate02() = 0;
    virtual const char* getNoOrderlineInsert() = 0;
    // Payment:
    virtual const char* getPmWarehouseSelect() = 0;
    virtual const char* getPmWarehouseUpdate() = 0;
    virtual const char* getPmDistrictSelect() = 0;
    virtual const char* getPmDistrictUpdate() = 0;
    virtual const char* getPmCustomerSelect1() = 0;
    virtual const char* getPmCustomerSelect2() = 0;
    virtual const char* getPmCustomerSelect3() = 0;
    virtual const char* getPmCustomerUpdate1() = 0;
    virtual const char* getPmCustomerSelect4() = 0;
    virtual const char* getPmCustomerUpdate2() = 0;
    virtual const char* getPmHistoryInsert() = 0;
    // OrderStatus:
    virtual const char* getOsCustomerSelect1() = 0;
    virtual const char* getOsCustomerSelect2() = 0;
    virtual const char* getOsCustomerSelect3() = 0;
    virtual const char* getOsOrderSelect() = 0;
    virtual const char* getOsOrderlineSelect() = 0;
    // Delivery:
    virtual const char* getDlNewOrderSelect() = 0;
    virtual const char* getDlNewOrderDelete() = 0;
    virtual const char* getDlOrderSelect() = 0;
    virtual const char* getDlOrderUpdate() = 0;
    virtual const char* getDlOrderlineUpdate() = 0;
    virtual const char* getDlOrderlineSelect() = 0;
    virtual const char* getDlCustomerUpdate() = 0;
    // StockLevel:
    virtual const char* getSlDistrictSelect() = 0;
    virtual const char* getSlStockSelect() = 0;
};

#endif
