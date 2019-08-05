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

#ifndef HANADIALECT_H
#define HANADIALECT_H

#include "Dialect.h"

class HanaDialect : public Dialect {

	private:
		std::vector<const char*> dropExistingSchemaStatements = {
			"DROP SCHEMA \"TPCCH\" CASCADE"
		};

		std::vector<const char*> createSchemaStatements = {
			"CREATE SCHEMA \"TPCCH\"",

			"CREATE COLUMN TABLE \"TPCCH\".\"WAREHOUSE\" (\n"
			"	\"W_ID\" INTEGER CS_INT,\n"
			"	\"W_NAME\" CHAR(10) CS_FIXEDSTRING,\n"
			"	\"W_STREET_1\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"W_STREET_2\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"W_CITY\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"W_STATE\" CHAR(2) CS_FIXEDSTRING,\n"
			"	\"W_ZIP\" CHAR(9) CS_FIXEDSTRING,\n"
			"	\"W_TAX\" DECIMAL(4,4) CS_FIXED,\n"
			"	\"W_YTD\" DECIMAL(12,2) CS_FIXED,\n"
			"	PRIMARY KEY (\"W_ID\")\n"
			") UNLOAD PRIORITY 5",

			"CREATE COLUMN TABLE \"TPCCH\".\"DISTRICT\" (\n"
			"	\"D_ID\" TINYINT CS_INT,\n"
			"	\"D_W_ID\" INTEGER CS_INT,\n"
			"	\"D_NAME\" CHAR(10) CS_FIXEDSTRING,\n"
			"	\"D_STREET_1\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"D_STREET_2\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"D_CITY\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"D_STATE\" CHAR(2) CS_FIXEDSTRING,\n"
			"	\"D_ZIP\" CHAR(9) CS_FIXEDSTRING,\n"
			"	\"D_TAX\" DECIMAL(4,4) CS_FIXED,\n"
			"	\"D_YTD\" DECIMAL(12,2) CS_FIXED,\n"
			"	\"D_NEXT_O_ID\" INTEGER CS_INT,\n"
			"	PRIMARY KEY (\"D_W_ID\",\"D_ID\") \n"
			") UNLOAD PRIORITY 5",

			"CREATE INDEX \"FK_DISTRICT_WAREHOUSE\" ON "
			"\"TPCCH\".\"DISTRICT\" ( \"D_W_ID\" ASC ) NONLEAF PARTIAL KEY LENGTH 1",

			"CREATE COLUMN TABLE \"TPCCH\".\"CUSTOMER\" (\n"
			"	\"C_ID\" SMALLINT CS_INT,\n"
			"	\"C_D_ID\" TINYINT CS_INT,\n"
			"	\"C_W_ID\" INTEGER CS_INT,\n"
			"	\"C_FIRST\" CHAR(16) CS_FIXEDSTRING,\n"
			"	\"C_MIDDLE\" CHAR(2) CS_FIXEDSTRING,\n"
			"	\"C_LAST\" CHAR(16) CS_FIXEDSTRING,\n"
			"	\"C_STREET_1\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"C_STREET_2\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"C_CITY\" CHAR(20) CS_FIXEDSTRING,\n"
			"	\"C_STATE\" CHAR(2) CS_FIXEDSTRING,\n"
			"	\"C_ZIP\" CHAR(9) CS_FIXEDSTRING,\n"
			"	\"C_PHONE\" CHAR(16) CS_FIXEDSTRING,\n"
			"	\"C_SINCE\" SECONDDATE CS_SECONDDATE,\n"
			"	\"C_CREDIT\" CHAR(2) CS_FIXEDSTRING,\n"
			"	\"C_CREDIT_LIM\" DECIMAL(12,2) CS_FIXED,\n"
			"	\"C_DISCOUNT\" DECIMAL(4,4) CS_FIXED,\n"
			"	\"C_BALANCE\" DECIMAL(12,2) CS_FIXED,\n"
			"	\"C_YTD_PAYMENT\" DECIMAL(12,2) CS_FIXED,\n"
			"	\"C_PAYMENT_CNT\" SMALLINT CS_INT,\n"
			"	\"C_DELIVERY_CNT\" SMALLINT CS_INT,\n"
			"	\"C_DATA\" CHAR(500) CS_FIXEDSTRING,\n"
			"	\"C_N_NATIONKEY\" INTEGER CS_INT,\n"
			"	PRIMARY KEY(\"C_W_ID\",\"C_D_ID\",\"C_ID\")\n"
			") UNLOAD PRIORITY 5",

			"ALTER TABLE \"TPCCH\".\"CUSTOMER\" WITH PARAMETERS "
			"('CONCAT_ATTRIBUTE'=('$C_W_ID$C_D_ID$','C_W_ID','C_D_ID'))",

			"CREATE INDEX \"FK_CUSTOMER_DISTRICT\" ON \"TPCCH\".\"CUSTOMER\" "
			"( \"C_W_ID\" ASC,\"C_D_ID\" ASC ) NONLEAF PARTIAL KEY LENGTH 1",

			"CREATE COLUMN TABLE \"TPCCH\".\"HISTORY\" (\n"
			"	\"H_C_ID\" SMALLINT CS_INT,\n"
			"	\"H_C_D_ID\" TINYINT CS_INT,\n"
			"	\"H_C_W_ID\" INTEGER CS_INT,\n"
			"	\"H_D_ID\" TINYINT CS_INT,\n"
			"	\"H_W_ID\" INTEGER CS_INT,\n"
			"	\"H_DATE\" SECONDDATE CS_SECONDDATE,\n"
			"	\"H_AMOUNT\" DECIMAL(6,2) CS_FIXED,\n"
			"	\"H_DATA\" CHAR(24) CS_FIXEDSTRING\n"
			") UNLOAD PRIORITY 5",

			"ALTER TABLE \"TPCCH\".\"HISTORY\" WITH PARAMETERS ('CONCAT_ATTRIBUTE'="
			"('$H_C_W_ID$H_C_D_ID$H_C_ID$','H_C_W_ID','H_C_D_ID','H_C_ID'))",

			"ALTER TABLE \"TPCCH\".\"HISTORY\" WITH PARAMETERS ('CONCAT_ATTRIBUTE'="
			"('$H_W_ID$H_D_ID$','H_W_ID','H_D_ID'))",

			"CREATE INDEX \"FK_HISTORY_CUSTOMER\" ON \"TPCCH\".\"HISTORY\" "
			"( \"H_C_W_ID\" ASC,\"H_C_D_ID\" ASC,\"H_C_ID\" ASC ) NONLEAF PARTIAL KEY LENGTH 1",

			"CREATE INDEX \"FK_HISTORY_DISTRICT\" ON \"TPCCH\".\"HISTORY\" "
			"( \"H_W_ID\" ASC,\"H_D_ID\" ASC ) NONLEAF PARTIAL KEY LENGTH 1",

			"CREATE COLUMN TABLE \"TPCCH\".\"NEWORDER\" (\n"
			"	\"NO_O_ID\" INTEGER CS_INT,\n"
			"	\"NO_D_ID\" TINYINT CS_INT,\n"
			"	\"NO_W_ID\" INTEGER CS_INT,\n"
			"	PRIMARY KEY (\"NO_W_ID\",\"NO_D_ID\",\"NO_O_ID\")\n"
			") UNLOAD PRIORITY 5",

			"CREATE COLUMN TABLE \"TPCCH\".\"ORDER\" (\n"
			"	\"O_ID\" INTEGER CS_INT,\n"
			"	\"O_D_ID\" TINYINT CS_INT,\n"
			"	\"O_W_ID\" INTEGER CS_INT,\n"
			"	\"O_C_ID\" SMALLINT CS_INT,\n"
			"	\"O_ENTRY_D\" SECONDDATE CS_SECONDDATE,\n"
			"	\"O_CARRIER_ID\" TINYINT CS_INT,\n"
			"	\"O_OL_CNT\" TINYINT CS_INT,\n"
			"	\"O_ALL_LOCAL\" TINYINT CS_INT,\n"
			"	PRIMARY KEY (\"O_W_ID\",\"O_D_ID\",\"O_ID\")\n"
			") UNLOAD PRIORITY 5",

			"ALTER TABLE \"TPCCH\".\"ORDER\" WITH PARAMETERS "
			"('CONCAT_ATTRIBUTE'=('$O_W_ID$O_D_ID$O_C_ID$','O_W_ID','O_D_ID','O_C_ID'))",

			"CREATE INDEX \"FK_ORDER_CUSTOMER\" ON \"TPCCH\".\"ORDER\" "
			"( \"O_W_ID\" ASC,\"O_D_ID\" ASC,\"O_C_ID\" ASC ) NONLEAF PARTIAL KEY LENGTH 1",

			"CREATE COLUMN TABLE \"TPCCH\".\"ORDERLINE\" (\n"
			"	\"OL_O_ID\" INTEGER CS_INT,\n"
			"	\"OL_D_ID\" TINYINT CS_INT,\n"
			"	\"OL_W_ID\" INTEGER CS_INT,\n"
			"	\"OL_NUMBER\" TINYINT CS_INT,\n"
			"	\"OL_I_ID\" INTEGER CS_INT,\n"
			"	\"OL_SUPPLY_W_ID\" INTEGER CS_INT,\n"
			"	\"OL_DELIVERY_D\" SECONDDATE CS_SECONDDATE,\n"
			"	\"OL_QUANTITY\" SMALLINT CS_INT,\n"
			"	\"OL_AMOUNT\" DECIMAL(6,2) CS_FIXED,\n"
			"	\"OL_DIST_INFO\" CHAR(24) CS_FIXEDSTRING,\n"
			"	PRIMARY KEY(\"OL_W_ID\",\"OL_D_ID\",\"OL_O_ID\",\"OL_NUMBER\")\n"
			") UNLOAD PRIORITY 5",

			"ALTER TABLE \"TPCCH\".\"ORDERLINE\" WITH PARAMETERS "
			"('CONCAT_ATTRIBUTE'=('$OL_W_ID$OL_D_ID$OL_O_ID$','OL_W_ID','OL_D_ID','OL_O_ID'))",

			"ALTER TABLE \"TPCCH\".\"ORDERLINE\" WITH PARAMETERS "
			"('CONCAT_ATTRIBUTE'=('$OL_SUPPLY_W_ID$OL_I_ID$','OL_SUPPLY_W_ID','OL_I_ID'))",

			"CREATE INDEX \"FK_ORDERLINE_ORDER\" ON \"TPCCH\".\"ORDERLINE\" "
			"( \"OL_W_ID\" ASC,\"OL_D_ID\" ASC,\"OL_O_ID\" ASC ) NONLEAF PARTIAL KEY LENGTH 1",

			"CREATE INDEX \"FK_ORDERLINE_STOCK\" ON \"TPCCH\".\"ORDERLINE\" "
			"( \"OL_SUPPLY_W_ID\" ASC,\"OL_I_ID\" ASC ) NONLEAF PARTIAL KEY LENGTH 1",

			"CREATE COLUMN TABLE \"TPCCH\".\"ITEM\" (\n"
			"	\"I_ID\" INTEGER CS_INT,\n"
			"	\"I_IM_ID\" SMALLINT CS_INT,\n"
			"	\"I_NAME\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"I_PRICE\" DECIMAL(5,2) CS_FIXED,\n"
			"	\"I_DATA\" CHAR(50) CS_FIXEDSTRING,\n"
			"	PRIMARY KEY(\"I_ID\")\n"
			") UNLOAD PRIORITY 5",

			"CREATE COLUMN TABLE \"TPCCH\".\"STOCK\" (\n"
			"	\"S_I_ID\" INTEGER CS_INT,\n"
			"	\"S_W_ID\" INTEGER CS_INT,\n"
			"	\"S_QUANTITY\" SMALLINT CS_INT,\n"
			"	\"S_DIST_01\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_02\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_03\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_04\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_05\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_06\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_07\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_08\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_09\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_DIST_10\" CHAR(24) CS_FIXEDSTRING,\n"
			"	\"S_YTD\" INTEGER CS_INT,\n"
			"	\"S_ORDER_CNT\" SMALLINT CS_INT,\n"
			"	\"S_REMOTE_CNT\" SMALLINT CS_INT,\n"
			"	\"S_DATA\" CHAR(50) CS_FIXEDSTRING,\n"
			"	\"S_SU_SUPPKEY\" INTEGER CS_INT,\n"
			"	PRIMARY KEY (\"S_W_ID\",\"S_I_ID\")\n"
			") UNLOAD PRIORITY 5",

			"CREATE INDEX \"FK_STOCK_WAREHOUSE\" ON \"TPCCH\".\"STOCK\" "
			"( \"S_W_ID\" ASC ) NONLEAF PARTIAL KEY LENGTH 1",

			"CREATE INDEX \"FK_STOCK_ITEM\" ON \"TPCCH\".\"STOCK\" "
			"( \"S_I_ID\" ASC ) NONLEAF PARTIAL KEY LENGTH 1",

			"CREATE COLUMN TABLE \"TPCCH\".\"NATION\" (\n"
			"	\"N_NATIONKEY\" TINYINT CS_INT NOT NULL,\n"
			"	\"N_NAME\" CHAR(25) CS_FIXEDSTRING NOT NULL,\n"
			"	\"N_REGIONKEY\" TINYINT CS_INT NOT NULL,\n"
			"	\"N_COMMENT\" CHAR(152) CS_FIXEDSTRING NOT NULL,\n"
			"	PRIMARY KEY (\"N_NATIONKEY\")\n"
			") UNLOAD PRIORITY 5",

			"CREATE COLUMN TABLE \"TPCCH\".\"SUPPLIER\" (\n"
			"	\"SU_SUPPKEY\" SMALLINT CS_INT NOT NULL,\n"
			"	\"SU_NAME\" CHAR(25) CS_FIXEDSTRING NOT NULL,\n"
			"	\"SU_ADDRESS\" CHAR(40) CS_FIXEDSTRING NOT NULL,\n"
			"	\"SU_NATIONKEY\"TINYINT CS_INT NOT NULL,\n"
			"	\"SU_PHONE\" CHAR(15) CS_FIXEDSTRING NOT NULL,\n"
			"	\"SU_ACCTBAL\" DECIMAL(12,2) CS_FIXED NOT NULL,\n"
			"	\"SU_COMMENT\" CHAR(101) CS_FIXEDSTRING NOT NULL,\n"
			"	PRIMARY KEY(\"SU_SUPPKEY\")\n"
			") UNLOAD PRIORITY 5",

			"CREATE COLUMN TABLE \"TPCCH\".\"REGION\" (\n"
			"	\"R_REGIONKEY\" TINYINT CS_INT NOT NULL,\n"
			"	\"R_NAME\" CHAR(55) CS_FIXEDSTRING NOT NULL,\n"
			"	\"R_COMMENT\" CHAR(152) CS_FIXEDSTRING NOT NULL,\n"
			"	PRIMARY KEY (\"R_REGIONKEY\")\n"
			") UNLOAD PRIORITY 5"
		};

		std::vector<const char*> additionalPreparationStatements = {
			"load TPCCH.\"WAREHOUSE\" all",
			"merge delta of TPCCH.\"WAREHOUSE\"",
			"load TPCCH.\"DISTRICT\" all",
			"merge delta of TPCCH.\"DISTRICT\"",
			"load TPCCH.\"CUSTOMER\" all",
			"merge delta of TPCCH.\"CUSTOMER\"",
			"load TPCCH.\"HISTORY\" all",
			"merge delta of TPCCH.\"HISTORY\"",
			"load TPCCH.\"NEWORDER\" all",
			"merge delta of TPCCH.\"NEWORDER\"",
			"load TPCCH.\"ORDER\" all",
			"merge delta of TPCCH.\"ORDER\"",
			"load TPCCH.\"ORDERLINE\" all",
			"merge delta of TPCCH.\"ORDERLINE\"",
			"load TPCCH.\"ITEM\" all",
			"merge delta of TPCCH.\"ITEM\"",
			"load TPCCH.\"STOCK\" all",
			"merge delta of TPCCH.\"STOCK\"",
			"load TPCCH.\"NATION\" all",
			"merge delta of TPCCH.\"NATION\"",
			"load TPCCH.\"SUPPLIER\" all",
			"merge delta of TPCCH.\"SUPPLIER\"",
			"load TPCCH.\"REGION\" all",
			"merge delta of TPCCH.\"REGION\"",
		};

		std::vector<const char*> importPrefixStrings ={
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '",
			"IMPORT FROM CSV FILE '"
		};

		std::vector<const char*> importSuffixStrings = {
			"/WAREHOUSE.tbl' INTO TPCCH.\"WAREHOUSE\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/DISTRICT.tbl' INTO TPCCH.\"DISTRICT\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/CUSTOMER.tbl' INTO TPCCH.\"CUSTOMER\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/HISTORY.tbl' INTO TPCCH.\"HISTORY\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/NEWORDER.tbl' INTO TPCCH.\"NEWORDER\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/ORDER.tbl' INTO TPCCH.\"ORDER\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/ORDERLINE.tbl' INTO TPCCH.\"ORDERLINE\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/ITEM.tbl' INTO TPCCH.\"ITEM\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/STOCK.tbl' INTO TPCCH.\"STOCK\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/NATION.tbl' INTO TPCCH.\"NATION\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/SUPPLIER.tbl' INTO TPCCH.\"SUPPLIER\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK",
			"/REGION.tbl' INTO TPCCH.\"REGION\" WITH RECORD DELIMITED BY '\n' FIELD DELIMITED BY '|' THREADS 10 TABLE LOCK"
		};

		std::vector<const char*> tpchQueryStrings = {
			//TPC-H-Query 1
			"select\n"
			"	ol_number,\n"
			"	sum(ol_quantity) as sum_qty,\n"
			"	sum(ol_amount) as sum_amount,\n"
			"	avg(ol_quantity) as avg_qty,"
			"	avg(ol_amount) as avg_amount,\n"
			"	count(*) as count_order\n"
			"from\n"
			"	TPCCH.ORDERLINE\n"
			"where\n"
			"	ol_delivery_d > '2007-01-02 00:00:00.000000'\n"
			"group by\n"
			"	ol_number\n"
			"order by\n"
			"	ol_number",

			//TPC-H-Query 2
			"select\n"
			"	su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment\n"
			"from\n"
			"	TPCCH.item, TPCCH.supplier, TPCCH.stock, TPCCH.nation, TPCCH.region,\n"
			"	(	select\n"
			"			s_i_id as m_i_id,\n"
			" 			min(s_quantity) as m_s_quantity\n"
			"		from\n"
			"			TPCCH.stock, TPCCH.supplier, TPCCH.nation, TPCCH.region\n"
			"		where\n"
			"				s_su_suppkey = su_suppkey\n"
			"			and su_nationkey = n_nationkey\n"
			"			and n_regionkey = r_regionkey\n"
			"			and r_name like 'EUROP%'\n"
			"		group by\n"
			"			s_i_id\n"
			"	) m\n"
			"where\n"
			"		i_id = s_i_id\n"
			"	and s_su_suppkey = su_suppkey\n"
			"	and su_nationkey = n_nationkey\n"
			"	and n_regionkey = r_regionkey\n"
			"	and i_data like '%b'\n"
			"	and r_name like 'EUROP%'\n"
			"	and i_id = m_i_id\n"
			"	and s_quantity = m_s_quantity\n"
			"order by\n"
			"	n_name, su_name, i_id",

			//TPC-H-Query 3
			"select\n"
			"	ol_o_id, ol_w_id, ol_d_id,\n"
			"	sum(ol_amount) as revenue, o_entry_d\n"
			"from\n"
			"	TPCCH.customer, TPCCH.neworder, TPCCH.\"ORDER\", TPCCH.orderline\n"
			"where\n"
			"		c_state like 'A%'\n"
			"	and c_id = o_c_id\n"
			"	and c_w_id = o_w_id\n"
			"	and c_d_id = o_d_id\n"
			"	and no_w_id = o_w_id\n"
			"	and no_d_id = o_d_id\n"
			"	and no_o_id = o_id\n"
			"	and ol_w_id = o_w_id\n"
			"	and ol_d_id = o_d_id\n"
			"	and ol_o_id = o_id\n"
			"	and o_entry_d > '2007-01-02 00:00:00.000000'\n"
			"group by\n"
			"	ol_o_id, ol_w_id, ol_d_id, o_entry_d\n"
			"order by\n"
			"	revenue desc, o_entry_d",

			//TPC-H-Query 4
			"select\n"
			"	o_ol_cnt, count(*) as order_count\n"
			"from\n"
			"	TPCCH.\"ORDER\"\n"
			"where\n"
			"		o_entry_d >= '2007-01-02 00:00:00.000000'\n"
			"	and o_entry_d < '2012-01-02 00:00:00.000000'\n"
			"	and exists \n"
			"		(	select *\n"
			"			from TPCCH.orderline\n"
			"			where 	o_id = ol_o_id\n"
			"	    		and o_w_id = ol_w_id\n"
			"	    		and o_d_id = ol_d_id\n"
			"	    		and ol_delivery_d >= o_entry_d)\n"
			"group by\n"
			"	o_ol_cnt\n"
			"order by\n"
			"	o_ol_cnt",

			//TPC-H-Query 5
			"select\n"
			"	n_name,\n"
			"	sum(ol_amount) as revenue\n"
			"from\n"
			"	TPCCH.customer, TPCCH.\"ORDER\", TPCCH.orderline, TPCCH.stock, TPCCH.supplier, TPCCH.nation, TPCCH.region\n"
			"where\n"
			"		c_id = o_c_id\n"
			"	and c_w_id = o_w_id\n"
			"	and c_d_id = o_d_id\n"
			"	and ol_o_id = o_id\n"
			"	and ol_w_id = o_w_id\n"
			"	and ol_d_id=o_d_id\n"
			"	and ol_w_id = s_w_id\n"
			"	and ol_i_id = s_i_id\n"
			"	and s_su_suppkey = su_suppkey\n"
			"	and c_n_nationkey = su_nationkey\n"
			"	and su_nationkey = n_nationkey\n"
			"	and n_regionkey = r_regionkey\n"
			"	and r_name = 'EUROPE'\n"
			"	and o_entry_d >= '2007-01-02 00:00:00.000000'\n"
			"group by\n"
			"		n_name\n"
			"order by\n"
			"	revenue desc",

			//TPC-H-Query 6
			"select\n"
			"	sum(ol_amount) as revenue\n"
			"from\n"
			"	TPCCH.orderline\n"
			"where\n"
			"		ol_delivery_d >= '1999-01-01 00:00:00.000000'\n"
			"	and ol_delivery_d < '2020-01-01 00:00:00.000000'\n"
			"	and ol_quantity between 1 and 100000",

			//TPC-H-Query 7
			"select\n"
			"	su_nationkey as supp_nation,\n"
			"	substr(c_state,1,1) as cust_nation,\n"
			"	extract(year from o_entry_d) as l_year,\n"
			"	sum(ol_amount) as revenue\n"
			"from\n"
			"	TPCCH.supplier, TPCCH.stock, TPCCH.orderline, TPCCH.\"ORDER\", TPCCH.customer, TPCCH.nation n1, TPCCH.nation n2\n"
			"where\n"
			"		ol_supply_w_id = s_w_id\n"
			"	and ol_i_id = s_i_id\n"
			"	and s_su_suppkey = su_suppkey\n"
			"	and ol_w_id = o_w_id\n"
			"	and ol_d_id = o_d_id\n"
			"	and ol_o_id = o_id\n"
			"	and c_id = o_c_id\n"
			"	and c_w_id = o_w_id\n"
			"	and c_d_id = o_d_id\n"
			"	and su_nationkey = n1.n_nationkey\n"
			"	and c_n_nationkey = n2.n_nationkey\n"
			"	and (\n"
			"		(n1.n_name = 'GERMANY' and n2.n_name = 'CAMBODIA')\n"
			"		or\n"
			"		(n1.n_name = 'CAMBODIA' and n2.n_name = 'GERMANY')\n"
			"		)\n"
			"	and ol_delivery_d between '2007-01-02 00:00:00.000000' and '2012-01-02 00:00:00.000000'\n"
			"group by\n"
			"	su_nationkey, substr(c_state,1,1), extract(year from o_entry_d)\n"
			"order by\n"
			"	su_nationkey, cust_nation, l_year",

			//TPC-H-Query 8
			"select\n"
			"	extract(year from o_entry_d) as l_year,\n"
			"	sum(case when n2.n_name = 'GERMANY' then ol_amount else 0 end) / sum(ol_amount) as mkt_share\n"
			"from\n"
			"	TPCCH.item, TPCCH.supplier, TPCCH.stock, TPCCH.orderline, TPCCH.\"ORDER\", TPCCH.customer, TPCCH.nation n1, TPCCH.nation n2, TPCCH.region\n"
			"where\n"
			"		i_id = s_i_id\n"
			"	and ol_i_id = s_i_id\n"
			"	and ol_supply_w_id = s_w_id\n"
			"	and s_su_suppkey = su_suppkey\n"
			"	and ol_w_id = o_w_id\n"
			"	and ol_d_id = o_d_id\n"
			"	and ol_o_id = o_id\n"
			"	and c_id = o_c_id\n"
			"	and c_w_id = o_w_id\n"
			"	and c_d_id = o_d_id\n"
			"	and n1.n_nationkey = c_n_nationkey\n"
			"	and n1.n_regionkey = r_regionkey\n"
			"	and ol_i_id < 1000\n"
			"	and r_name = 'EUROPE'\n"
			"	and su_nationkey = n2.n_nationkey\n"
			"	and o_entry_d between '2007-01-02 00:00:00.000000' and '2012-01-02 00:00:00.000000'\n"
			"	and i_data like '%b'\n"
			"	and i_id = ol_i_id\n"
			"group by\n"
			"	extract(year from o_entry_d)\n"
			"order by\n"
			"	l_year",

			//TPC-H-Query 9
			"select\n"
			"	n_name, extract(year from o_entry_d) as l_year, sum(ol_amount) as sum_profit\n"
			"from\n"
			"	TPCCH.item, TPCCH.stock, TPCCH.supplier, TPCCH.orderline, TPCCH.\"ORDER\", TPCCH.nation\n"
			"where\n"
			"		ol_i_id = s_i_id\n"
			"	and ol_supply_w_id = s_w_id\n"
			"	and s_su_suppkey = su_suppkey\n"
			"	and ol_w_id = o_w_id\n"
			"	and ol_d_id = o_d_id\n"
			"	and ol_o_id = o_id\n"
			"	and ol_i_id = i_id\n"
			"	and su_nationkey = n_nationkey\n"
			"	and i_data like '%BB'\n"
			"group by\n"
			"	n_name, extract(year from o_entry_d)\n"
			"order by\n"
			"	n_name, l_year desc",

			//TPC-H-Query 10
			"select\n"
			"	c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name\n"
			"from\n"
			"	TPCCH.customer, TPCCH.\"ORDER\", TPCCH.orderline, TPCCH.nation\n"
			"where\n"
			"		c_id = o_c_id\n"
			"	and c_w_id = o_w_id\n"
			"	and c_d_id = o_d_id\n"
			"	and ol_w_id = o_w_id\n"
			"	and ol_d_id = o_d_id\n"
			"	and ol_o_id = o_id\n"
			"	and o_entry_d >= '2007-01-02 00:00:00.000000'\n"
			"	and o_entry_d <= ol_delivery_d\n"
			"	and n_nationkey = c_n_nationkey\n"
			"group by\n"
			"	c_id, c_last, c_city, c_phone, n_name\n"
			"order by\n"
			"	revenue desc",

			//TPC-H-Query 11
			"select\n"
			"	s_i_id, sum(s_order_cnt) as ordercount\n"
			"from\n"
			"	TPCCH.stock, TPCCH.supplier, TPCCH.nation\n"
			"where\n"
			"		s_su_suppkey = su_suppkey\n"
			"	and su_nationkey = n_nationkey\n"
			"	and n_name = 'GERMANY'\n"
			"group by\n"
			"	s_i_id\n"
			"having \n"
			"	sum(s_order_cnt) > (\n"
			"		select\n"
			"			sum(s_order_cnt) * .005\n"
			"		from\n"
			"			TPCCH.stock, TPCCH.supplier, TPCCH.nation\n"
			"		where\n"
			"				s_su_suppkey = su_suppkey\n"
			"			and su_nationkey = n_nationkey\n"
			"			and n_name = 'GERMANY')\n"
			"order by\n"
			"	ordercount desc",

			//TPC-H-Query 12
			"select\n"
			"	o_ol_cnt,\n"
			"	sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,\n"
			"	sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count\n"
			"from\n"
			"	TPCCH.\"ORDER\", TPCCH.orderline\n"
			"where\n"
			"		ol_w_id = o_w_id\n"
			"	and ol_d_id = o_d_id\n"
			"	and ol_o_id = o_id\n"
			"	and o_entry_d <= ol_delivery_d\n"
			"	and ol_delivery_d < '2020-01-01 00:00:00.000000'\n"
			"group by\n"
			"	o_ol_cnt\n"
			"order by\n"
			"	o_ol_cnt",

			//TPC-H-Query 13
			"select\n"
			"	c_count, count(*) as custdist\n"
			"from\n"
			"	(	select\n"
			"			c_id, count(o_id) as c_count\n"
			"		from\n"
			"			TPCCH.customer left outer join TPCCH.\"ORDER\" on (\n"
			"				c_w_id = o_w_id\n"
			"			and c_d_id = o_d_id\n"
			"			and c_id = o_c_id\n"
			"			and o_carrier_id > 8)\n"
			"	 	group by\n"
			"	 		c_id\n"
			"	 ) as c_orders\n"
			"group by\n"
			"	c_count\n"
			"order by\n"
			"	custdist desc, c_count desc",

			//TPC-H-Query 14
			"select\n"
			"	100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1+sum(ol_amount)) as promo_revenue\n"
			"from\n"
			"	TPCCH.orderline, TPCCH.item\n"
			"where\n"
			"		ol_i_id = i_id\n"
			"	and ol_delivery_d >= '2007-01-02 00:00:00.000000'\n"
			"	and ol_delivery_d < '2020-01-02 00:00:00.000000'",

			//TPC-H-Query 15
			"select\n"
			"	su_suppkey, su_name, su_address, su_phone, total_revenue\n"
			"from\n"
			"	TPCCH.supplier,\n"
			"		(select\n"
			"			s_su_suppkey as supplier_no,\n"
			"			sum(ol_amount) as total_revenue\n"
			"	 	from\n"
			"	 		TPCCH.orderline, TPCCH.stock\n"
			"		where\n"
			"				ol_i_id = s_i_id\n"
			"			and ol_supply_w_id = s_w_id\n"
			"			and ol_delivery_d >= '2007-01-02 00:00:00.000000'\n"
			"	 	group by\n"
			"	 		s_su_suppkey\n"
			"		) as revenue\n"
			"where\n"
			"		su_suppkey = supplier_no\n"
			"	and total_revenue = (\n"
			"		select max(total_revenue)\n"
			"		from\n"
			"			(select\n"
			"				s_su_suppkey as supplier_no,\n"
			"				sum(ol_amount) as total_revenue\n"
			"	 		from\n"
			"	 			TPCCH.orderline, TPCCH.stock\n"
			"			where\n"
			"					ol_i_id = s_i_id\n"
			"				and ol_supply_w_id = s_w_id\n"
			"				and ol_delivery_d >= '2007-01-02 00:00:00.000000'\n"
			"	 		group by\n"
			"	 			s_su_suppkey\n"
			"		) as revenue\n"
			"	)\n"
			"order by\n"
			"	su_suppkey",

			//TPC-H-Query 16
			"select\n"
			"	i_name,\n"
			"	substr(i_data, 1, 3) as brand,\n"
			"	i_price,\n"
			"	count(distinct s_su_suppkey) as supplier_cnt\n"
			"from\n"
			"	TPCCH.stock, TPCCH.item\n"
			"where\n"
			"		i_id = s_i_id\n"
			"	and i_data not like 'zz%'\n"
			"	and (s_su_suppkey not in\n"
			"		(	select\n"
			"				su_suppkey\n"
			"		 	from\n"
			"		 		TPCCH.supplier\n"
			"		 	where\n"
			"		 su_comment like '%bad%')\n"
			"		)\n"
			"group by\n"
			"	i_name, substr(i_data, 1, 3), i_price\n"
			"order by\n"
			"	supplier_cnt desc",

			//TPC-H-Query 17
			"select\n"
			"	sum(ol_amount) / 2.0 as avg_yearly\n"
			"from\n"
			"	TPCCH.orderline,\n"
			"	(	select\n"
			"			i_id, avg(ol_quantity) as a\n"
			"		from\n"
			"			TPCCH.item, TPCCH.orderline\n"
			"		    where\n"
			"		    		i_data like '%b'\n"
			"				and ol_i_id = i_id\n"
			"		    group by\n"
			"		    	i_id\n"
			"	) t\n"
			"where\n"
			"		ol_i_id = t.i_id\n"
			"	and ol_quantity < t.a",

			//TPC-H-Query 18
			"select\n"
			"	c_last, c_id, o_id, o_entry_d, o_ol_cnt, sum(ol_amount)\n"
			"from\n"
			"	TPCCH.customer, TPCCH.\"ORDER\", TPCCH.orderline\n"
			"where\n"
			"		c_id = o_c_id\n"
			"	and c_w_id = o_w_id\n"
			"	and c_d_id = o_d_id\n"
			"	and ol_w_id = o_w_id\n"
			"	and ol_d_id = o_d_id\n"
			"	and ol_o_id = o_id\n"
			"group by\n"
			"	o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt\n"
			"having\n"
			"	sum(ol_amount) > 200\n"
			"order by\n"
			"	sum(ol_amount) desc, o_entry_d",

			//TPC-H-Query 19
			"select\n"
			"	sum(ol_amount) as revenue\n"
			"from\n"
			"	TPCCH.orderline, TPCCH.item\n"
			"where\n"
			"	(\n"
			"		ol_i_id = i_id\n"
			"	and i_data like '%a'\n"
			"	and ol_quantity >= 1\n"
			"	and ol_quantity <= 10\n"
			"	and i_price between 1 and 400000\n"
			"	and ol_w_id in (1,2,3)\n"
			"	) or (\n"
			"		ol_i_id = i_id\n"
			"	and i_data like '%b'\n"
			"	and ol_quantity >= 1\n"
			"	and ol_quantity <= 10\n"
			"	and i_price between 1 and 400000\n"
			"	and ol_w_id in (1,2,4)\n"
			"	) or (\n"
			"		ol_i_id = i_id\n"
			"	and i_data like '%c'\n"
			"	and ol_quantity >= 1\n"
			"	and ol_quantity <= 10\n"
			"	and i_price between 1 and 400000\n"
			"	and ol_w_id in (1,5,3)\n"
			"	)",

			//TPC-H-Query 20
			"select	 su_name, su_address\n"
			"from	 TPCCH.supplier, TPCCH.nation\n"
			"where	 su_suppkey in\n"
			"		(select  mod(s_i_id * s_w_id, 10000)\n"
			"		from     TPCCH.stock, TPCCH.orderline\n"
			"		where    s_i_id in\n"
			"				(select i_id\n"
			"				 from TPCCH.item\n"
			"				 where i_data like 'co%')\n"
			"			 and ol_i_id=s_i_id\n"
			"			 and ol_delivery_d > '2010-05-23 12:00:00'\n"
			"		group by s_i_id, s_w_id, s_quantity\n"
			"		having   2*s_quantity > sum(ol_quantity))\n"
			"	 and su_nationkey = n_nationkey\n"
			"	 and n_name = 'GERMANY'\n"
			"order by su_name",

			//TPC-H-Query 21
			"select\n"
			"	su_name, count(*) as numwait\n"
			"from\n"
			"	TPCCH.supplier, TPCCH.orderline l1, TPCCH.\"ORDER\", TPCCH.stock, TPCCH.nation\n"
			"where\n"
			"		ol_o_id = o_id\n"
			"	and ol_w_id = o_w_id\n"
			"	and ol_d_id = o_d_id\n"
			"	and ol_w_id = s_w_id\n"
			"	and ol_i_id = s_i_id\n"
			"	and s_su_suppkey = su_suppkey\n"
			"	and l1.ol_delivery_d > o_entry_d\n"
			"	and not exists (\n"
			"		select *\n"
			"		from\n"
			"			TPCCH.orderline l2\n"
			"		where\n"
			"				l2.ol_o_id = l1.ol_o_id\n"
			"			and l2.ol_w_id = l1.ol_w_id\n"
			"			and l2.ol_d_id = l1.ol_d_id\n"
			"			and l2.ol_delivery_d > l1.ol_delivery_d\n"
			"		)\n"
			"	and su_nationkey = n_nationkey\n"
			"	and n_name = 'GERMANY'\n"
			"group by\n"
			"	su_name\n"
			"order by\n"
			"	numwait desc, su_name",

			//TPC-H-Query 22
			"select\n"
			"	substr(c_state,1,1) as country,\n"
			"	count(*) as numcust,\n"
			"	sum(c_balance) as totacctbal\n"
			"from\n"
			"	TPCCH.customer\n"
			"where\n"
			"		substr(c_phone,1,1) in ('1','2','3','4','5','6','7')\n"
			"	and c_balance > (\n"
			"		select\n"
			"			avg(c_BALANCE)\n"
			"		from\n"
			"			TPCCH.customer\n"
			"		where\n"
			"				c_balance > 0.00\n"
			"			and substr(c_phone,1,1) in ('1','2','3','4','5','6','7')\n"
			"	)\n"
			"	and not exists (\n"
			"		select *\n"
			"		from\n"
			"			TPCCH.\"ORDER\"\n"
			"		where\n"
			"				o_c_id = c_id\n"
			"			and o_w_id = c_w_id\n"
			"			and o_d_id = c_d_id\n"
			"	)\n"
			"group by\n"
			"	substr(c_state,1,1)\n"
			"order by\n"
			"	substr(c_state,1,1)"
		};

	public:

		//Strings to create initial database
		virtual std::vector<const char*>& getDropExistingSchemaStatements(){
			return dropExistingSchemaStatements;
		}

		virtual std::vector<const char*>& getCreateSchemaStatements(){
			return createSchemaStatements;
		}

		virtual std::vector<const char*>& getImportPrefix(){
			return importPrefixStrings;
		}

		virtual std::vector<const char*>& getImportSuffix(){
			return importSuffixStrings;
		}


		virtual std::vector<const char*>& getAdditionalPreparationStatements(){
			return additionalPreparationStatements;
		}

		//22 adjusted TPC-H OLAP query strings
		virtual std::vector<const char*>& getTpchQueryStrings(){
			return tpchQueryStrings;
		}

		//Strings for database check
		virtual const char* getSelectCountWarehouse(){
			return "select count(*) from TPCCH.WAREHOUSE";
		}

		virtual const char* getSelectCountDistrict(){
			return "select count(*) from TPCCH.DISTRICT";
		}

		virtual const char* getSelectCountCustomer(){
			return "select count(*) from TPCCH.CUSTOMER";
		}

		virtual const char* getSelectCountOrder(){
			return "select count(*) from TPCCH.\"ORDER\"";
		}

		virtual const char* getSelectCountOrderline(){
			return "select count(*) from TPCCH.ORDERLINE";
		}

		virtual const char* getSelectCountNeworder(){
			return "select count(*) from TPCCH.NEWORDER";
		}

		virtual const char* getSelectCountHistory(){
			return "select count(*) from TPCCH.HISTORY";
		}

		virtual const char* getSelectCountStock(){
			return "select count(*) from TPCCH.STOCK";
		}

		virtual const char* getSelectCountItem(){
			return "select count(*) from TPCCH.ITEM";
		}

		virtual const char* getSelectCountSupplier(){
			return "select count(*) from TPCCH.SUPPLIER";
		}

		virtual const char* getSelectCountNation(){
			return "select count(*) from TPCCH.NATION";
		}

		virtual const char* getSelectCountRegion(){
			return "select count(*) from TPCCH.REGION";
		}

		//TPC-C transaction strings
		//NewOrder:
		virtual const char* getNoWarehouseSelect(){
			return "select W_TAX from TPCCH.WAREHOUSE where W_ID=?";
		}

		virtual const char* getNoDistrictSelect(){
			return "select D_TAX, D_NEXT_O_ID from TPCCH.DISTRICT where D_W_ID=? and D_ID=?";
		}

		virtual const char* getNoDistrictUpdate(){
			return "update TPCCH.DISTRICT set D_NEXT_O_ID=D_NEXT_O_ID+1 where D_W_ID=? and D_ID=?";
		}

		virtual const char* getNoCustomerSelect(){
			return "select C_DISCOUNT,C_LAST,C_CREDIT from TPCCH.CUSTOMER where C_W_ID=? and C_D_ID=? and C_ID=?";
		}

		virtual const char* getNoOrderInsert(){
			return "insert into TPCCH.\"ORDER\" values (?,?,?,?,?,NULL,?,?)";
		}

		virtual const char* getNoNewOrderInsert(){
			return "insert into TPCCH.NEWORDER values(?,?,?)";
		}

		virtual const char* getNoItemSelect(){
			return "select I_PRICE,I_NAME,I_DATA from TPCCH.ITEM where I_ID=?";
		}

		virtual const char* getNoStockSelect01(){
			return "select S_QUANTITY,S_DIST_01,S_DATA from TPCCH.STOCK where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockSelect02(){
			return "select S_QUANTITY,S_DIST_02,S_DATA from TPCCH.STOCK where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockSelect03(){
			return "select S_QUANTITY,S_DIST_03,S_DATA from TPCCH.STOCK where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockSelect04(){
			return "select S_QUANTITY,S_DIST_04,S_DATA from TPCCH.STOCK where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockSelect05(){
			return "select S_QUANTITY,S_DIST_05,S_DATA from TPCCH.STOCK where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockSelect06(){
			return "select S_QUANTITY,S_DIST_06,S_DATA from TPCCH.STOCK where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockSelect07(){
			return "select S_QUANTITY,S_DIST_07,S_DATA from TPCCH.STOCK where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockSelect08(){
			return "select S_QUANTITY,S_DIST_08,S_DATA from TPCCH.STOCK where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockSelect09(){
			return "select S_QUANTITY,S_DIST_09,S_DATA from TPCCH.STOCK where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockSelect10(){
			return "select S_QUANTITY,S_DIST_10,S_DATA from TPCCH.STOCK where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockUpdate01(){
			return "update TPCCH.STOCK set S_YTD=S_YTD+?, S_ORDER_CNT=S_ORDER_CNT+1, S_QUANTITY=? where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoStockUpdate02(){
			return "update TPCCH.STOCK set S_YTD=S_YTD+?, S_ORDER_CNT=S_ORDER_CNT+1, S_QUANTITY=?, S_REMOTE_CNT=S_REMOTE_CNT+1 where S_I_ID=? and S_W_ID=?";
		}

		virtual const char* getNoOrderlineInsert(){
			return "insert into TPCCH.ORDERLINE values (?,?,?,?,?,?,NULL,?,?,?)";
		}

		//Payment:
		virtual const char* getPmWarehouseSelect(){
			return "select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP from TPCCH.WAREHOUSE where W_ID=?";
		}

		virtual const char* getPmWarehouseUpdate(){
			return "update TPCCH.WAREHOUSE set W_YTD=W_YTD+? where W_ID=?";
		}

		virtual const char* getPmDistrictSelect(){
			return "select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP from TPCCH.DISTRICT where D_W_ID=? and D_ID=?";
		}

		virtual const char* getPmDistrictUpdate(){
			return "update TPCCH.DISTRICT set D_YTD=D_YTD+? where D_W_ID=? and D_ID=?";
		}

		virtual const char* getPmCustomerSelect1(){
			return "select count(*) from TPCCH.CUSTOMER where C_LAST=? and C_D_ID=? and C_W_ID=?";
		}

		virtual const char* getPmCustomerSelect2(){
			return "select C_ID, C_FIRST, C_MIDDLE, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE from TPCCH.CUSTOMER where C_LAST=? and C_D_ID=? and C_W_ID=? order by C_FIRST asc";
		}

		virtual const char* getPmCustomerSelect3(){
			return "select C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE from TPCCH.CUSTOMER where C_ID=? and C_D_ID=? and C_W_ID=?";
		}

		virtual const char* getPmCustomerUpdate1(){
			return "update TPCCH.CUSTOMER set C_BALANCE=C_BALANCE-?, C_YTD_PAYMENT=C_YTD_PAYMENT+?, C_PAYMENT_CNT=C_PAYMENT_CNT+1 where C_ID=? and C_D_ID=? and C_W_ID=?";
		}

		virtual const char* getPmCustomerSelect4(){
			return "select C_DATA from TPCCH.CUSTOMER where C_ID=? and C_D_ID=? and C_W_ID=?";
		}

		virtual const char* getPmCustomerUpdate2(){
			return "update TPCCH.CUSTOMER set C_DATA=? where C_ID=? and C_D_ID=? and C_W_ID=?";
		}

		virtual const char* getPmHistoryInsert(){
			return "insert into TPCCH.History values (?,?,?,?,?,?,?,?)";
		}

		//OrderStatus:
		virtual const char* getOsCustomerSelect1(){
			return "select count(*) from TPCCH.CUSTOMER where C_LAST=? and C_D_ID=? and C_W_ID=?";
		}

		virtual const char* getOsCustomerSelect2(){
			return "select C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST from TPCCH.CUSTOMER where C_LAST=? and C_D_ID=? and C_W_ID=? order by C_FIRST asc";
		}

		virtual const char* getOsCustomerSelect3(){
			return "select C_BALANCE, C_FIRST, C_MIDDLE, C_LAST from TPCCH.CUSTOMER where C_ID=? and C_D_ID=? and C_W_ID=?";
		}

		virtual const char* getOsOrderSelect(){
			return "select O_ID, O_ENTRY_D, O_CARRIER_ID from TPCCH.\"ORDER\" where O_W_ID=? and O_D_ID=? and O_C_ID=? and O_ID=(select max(O_ID) from TPCCH.\"ORDER\" where O_W_ID=? and O_D_ID=? and O_C_ID=?)";
		}

		virtual const char* getOsOrderlineSelect(){
			return "select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from TPCCH.ORDERLINE where OL_W_ID=? and OL_D_ID=? and OL_O_ID=?";
		}

		//Delivery:
		virtual const char* getDlNewOrderSelect(){
			return "select NO_O_ID from TPCCH.NEWORDER where NO_W_ID=? and NO_D_ID=? and NO_O_ID=(select min(NO_O_ID) from TPCCH.NEWORDER where NO_W_ID=? and NO_D_ID=?)";
		}

		virtual const char* getDlNewOrderDelete(){
			return "delete from TPCCH.NEWORDER where NO_W_ID=? and NO_D_ID=? and NO_O_ID=?";
		}

		virtual const char* getDlOrderSelect(){
			return "select O_C_ID from TPCCH.\"ORDER\" where O_W_ID=? and O_D_ID=? and O_ID=?";
		}

		virtual const char* getDlOrderUpdate(){
			return "update TPCCH.\"ORDER\" set O_CARRIER_ID=? where O_W_ID=? and O_D_ID=? and O_ID=?";
		}

		virtual const char* getDlOrderlineUpdate(){
			return "update TPCCH.ORDERLINE set OL_DELIVERY_D=? where OL_W_ID=? and OL_D_ID=? and OL_O_ID=?";
		}

		virtual const char* getDlOrderlineSelect(){
			return "select sum(OL_AMOUNT) from TPCCH.ORDERLINE where OL_W_ID=? and OL_D_ID=? and OL_O_ID=?";
		}

		virtual const char* getDlCustomerUpdate(){
			return "update TPCCH.CUSTOMER set C_BALANCE=C_BALANCE+?, C_DELIVERY_CNT=C_DELIVERY_CNT+1 where C_ID=? and C_D_ID=? and C_W_ID=?";
		}

		//StockLevel:
		virtual const char* getSlDistrictSelect(){
			return "select D_NEXT_O_ID from TPCCH.DISTRICT where D_W_ID=? and D_ID=?";
		}

		virtual const char* getSlStockSelect(){
			return "select count(*) from TPCCH.STOCK,(select distinct OL_I_ID from TPCCH.ORDERLINE where OL_W_ID=? and OL_D_ID=? and OL_O_ID<? and OL_O_ID>=?) where S_I_ID=OL_I_ID and S_W_ID=? and S_QUANTITY<?";
		}

};

#endif
