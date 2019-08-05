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

#ifndef TUPLEGEN_H
#define TUPLEGEN_H

#include "DataSource.h"

#include <fstream>
#include <string>

class TupleGen{
	
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
		static void openOutputFiles();
		static void closeOutputFiles();	
		static void genWarehouse(int& wId);
		static void genDistrict(int& dId, int& wId);
		static void genCustomer(int& cId, int& dId, int& wId, std::string& customerTime);
		static void genHistory(int& cId, int& dId, int& wId);
		static void genNeworder(int& oId, int& dId, int& wId);
		static void genOrder(int& oId, int& dId, int& wId, int& cId, int& olCount, std::string& orderTime);
		static void genOrderline(int& oId, int& dId, int& wId, int& olNumber, std::string& orderTime);
		static void genItem(int& iId);
		static void genStock(int& iId, int& wId);
		static void genNation(Nation n);
		static void genSupplier(int& suId);
		static void genRegion(int& rId, const char* rName);

};

#endif
