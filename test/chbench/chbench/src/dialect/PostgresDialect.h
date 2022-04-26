/*
Copyright 2019 Materialize, Inc

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

#ifndef POSTGRESDIALECT_H
#define POSTGRESDIALECT_H

#include "Dialect.h"

class PostgresDialect : public Dialect {

  private:
    std::vector<const char*> dropExistingSchemaStatements = {
        "DROP SCHEMA IF EXISTS tpcch CASCADE"};

    std::vector<const char*> createSchemaStatements = {
        "CREATE SCHEMA IF NOT EXISTS tpcch",

        "CREATE TABLE tpcch.warehouse (\n"
        "	w_id integer,\n"
        "	w_name char(10),\n"
        "	w_street_1 char(20),\n"
        "	w_street_2 char(20),\n"
        "	w_city char(20),\n"
        "	w_state char(2),\n"
        "	w_zip char(9),\n"
        "	w_tax decimal(4,4),\n"
        "	w_ytd decimal(12,2),\n"
        "	PRIMARY KEY (w_id)\n"
        ")",

        "ALTER TABLE tpcch.warehouse\n"
        "REPLICA IDENTITY full \n",

        "CREATE TABLE tpcch.district (\n"
        "	d_id smallint,\n"
        "	d_w_id integer,\n"
        "	d_name char(10),\n"
        "	d_street_1 char(20),\n"
        "	d_street_2 char(20),\n"
        "	d_city char(20),\n"
        "	d_state char(2),\n"
        "	d_zip char(9),\n"
        "	d_tax decimal(4,4),\n"
        "	d_ytd decimal(12,2),\n"
        "	d_next_o_id integer,\n"
        "	PRIMARY KEY (d_w_id, d_id) \n"
        ")",

        "ALTER TABLE tpcch.district\n"
        "REPLICA IDENTITY full \n",

        "CREATE INDEX fk_district_warehouse ON tpcch.district (d_w_id ASC)",

        "CREATE TABLE tpcch.customer (\n"
        "	c_id smallint,\n"
        "	c_d_id smallint,\n"
        "	c_w_id integer,\n"
        "	c_first char(16),\n"
        "	c_middle char(2),\n"
        "	c_last char(16),\n"
        "	c_street_1 char(20),\n"
        "	c_street_2 char(20),\n"
        "	c_city char(20),\n"
        "	c_state char(2),\n"
        "	c_zip char(9),\n"
        "	c_phone char(16),\n"
        "	c_since DATE,\n"
        "	c_credit char(2),\n"
        "	c_credit_lim decimal(12,2),\n"
        "	c_discount decimal(4,4),\n"
        "	c_balance decimal(12,2),\n"
        "	c_ytd_payment decimal(12,2),\n"
        "	c_payment_cnt smallint,\n"
        "	c_delivery_cnt smallint,\n"
        "	c_data text,\n"
        "	c_n_nationkey integer,\n"
        "	PRIMARY KEY(c_w_id, c_d_id, c_id)\n"
        ")",

        "ALTER TABLE tpcch.customer\n"
        "REPLICA IDENTITY full \n",

        "CREATE INDEX fk_customer_district ON tpcch.customer"
        "(c_w_id ASC, c_d_id ASC)",

        "CREATE TABLE tpcch.history (\n"
        "	h_c_id smallint,\n"
        "	h_c_d_id smallint,\n"
        "	h_c_w_id integer,\n"
        "	h_d_id smallint,\n"
        "	h_w_id integer,\n"
        "	h_date date,\n"
        "	h_amount decimal(6,2),\n"
        "	h_data char(24)\n"
        ")",

        "ALTER TABLE tpcch.history\n"
        "REPLICA IDENTITY full \n",

        "CREATE INDEX fk_history_customer ON tpcch.history "
        "(h_c_w_id ASC, h_c_d_id ASC, h_c_id ASC)",

        "CREATE INDEX fk_history_district ON tpcch.history "
        "(h_w_id ASC, h_d_id ASC)",

        "CREATE TABLE tpcch.neworder (\n"
        "	no_o_id integer,\n"
        "	no_d_id smallint,\n"
        "	no_w_id integer,\n"
        "	PRIMARY KEY (no_w_id, no_d_id, no_o_id)\n"
        ")",

        "ALTER TABLE tpcch.neworder\n"
        "REPLICA IDENTITY full \n",

        "CREATE TABLE tpcch.order (\n"
        "	o_id integer,\n"
        "	o_d_id smallint,\n"
        "	o_w_id integer,\n"
        "	o_c_id smallint,\n"
        "	o_entry_d date,\n"
        "	o_carrier_id smallint,\n"
        "	o_ol_cnt smallint,\n"
        "	o_all_local smallint,\n"
        "	PRIMARY KEY (o_w_id, o_d_id, o_id)\n"
        ")",

        "ALTER TABLE tpcch.order\n"
        "REPLICA IDENTITY full \n",

        "CREATE INDEX fk_order_customer ON tpcch.order "
        "(o_w_id ASC, o_d_id ASC, o_c_id ASC)",

        "CREATE TABLE tpcch.orderline (\n"
        "	ol_o_id integer,\n"
        "	ol_d_id smallint,\n"
        "	ol_w_id integer,\n"
        "	ol_number smallint,\n"
        "	ol_i_id integer,\n"
        "	ol_supply_w_id integer,\n"
        "	ol_delivery_d date,\n"
        "	ol_quantity smallint,\n"
        "	ol_amount decimal(6,2),\n"
        "	ol_dist_info char(24),\n"
        "	PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)\n"
        ")",

        "ALTER TABLE tpcch.orderline\n"
        "REPLICA IDENTITY full \n",

        "CREATE INDEX fk_orderline_order ON tpcch.orderline "
        "(ol_w_id ASC, ol_d_id ASC, ol_o_id ASC)",

        "CREATE INDEX fk_orderline_stock ON tpcch.orderline "
        "(ol_supply_w_id ASC, ol_i_id ASC)",

        "CREATE TABLE tpcch.item (\n"
        "	i_id integer,\n"
        "	i_im_id smallint,\n"
        "	i_name char(24),\n"
        "	i_price decimal(5,2),\n"
        "	i_data char(50),\n"
        "	PRIMARY KEY (i_id)\n"
        ")",

        "ALTER TABLE tpcch.item\n"
        "REPLICA IDENTITY full \n",


        "CREATE TABLE tpcch.stock (\n"
        "	s_i_id integer,\n"
        "	s_w_id integer,\n"
        "	s_quantity smallint,\n"
        "	s_dist_01 char(24),\n"
        "	s_dist_02 char(24),\n"
        "	s_dist_03 char(24),\n"
        "	s_dist_04 char(24),\n"
        "	s_dist_05 char(24),\n"
        "	s_dist_06 char(24),\n"
        "	s_dist_07 char(24),\n"
        "	s_dist_08 char(24),\n"
        "	s_dist_09 char(24),\n"
        "	s_dist_10 char(24),\n"
        "	s_ytd integer,\n"
        "	s_order_cnt smallint,\n"
        "	s_remote_cnt smallint,\n"
        "	s_data char(50),\n"
        "	s_su_suppkey integer,\n"
        "	PRIMARY KEY (s_w_id, s_i_id)\n"
        ")",

        "ALTER TABLE tpcch.stock\n"
        "REPLICA IDENTITY full \n",

        "CREATE INDEX fk_stock_warehouse ON tpcch.stock (s_w_id ASC)",

        "CREATE INDEX fk_stock_item ON tpcch.stock (s_i_id ASC)",

        "CREATE TABLE tpcch.nation (\n"
        "	n_nationkey smallint NOT NULL,\n"
        "	n_name char(25) NOT NULL,\n"
        "	n_regionkey smallint NOT NULL,\n"
        "	n_comment char(152) NOT NULL,\n"
        "	PRIMARY KEY (n_nationkey)\n"
        ")",

        "ALTER TABLE tpcch.nation\n"
        "REPLICA IDENTITY full \n",

        "CREATE TABLE tpcch.supplier (\n"
        "	su_suppkey smallint NOT NULL,\n"
        "	su_name char(25) NOT NULL,\n"
        "	su_address char(40) NOT NULL,\n"
        "	su_nationkey smallint NOT NULL,\n"
        "	su_phone char(15) NOT NULL,\n"
        "	su_acctbal decimal(12,2) NOT NULL,\n"
        "	su_comment char(101) NOT NULL,\n"
        "	PRIMARY KEY (su_suppkey)\n"
        ")",

        "ALTER TABLE tpcch.supplier\n"
        "REPLICA IDENTITY full \n",

        "CREATE TABLE tpcch.region (\n"
        "	r_regionkey smallint NOT NULL,\n"
        "	r_name char(55) NOT NULL,\n"
        "	r_comment char(152) NOT NULL,\n"
        "	PRIMARY KEY (r_regionkey)\n"
        ")",

        "ALTER TABLE tpcch.region \n"
            "REPLICA IDENTITY full \n"

        };

    std::vector<const char*> additionalPreparationStatements = {};

    std::vector<const char*> importPrefixStrings = {
        "COPY tpcch.warehouse FROM '",
        "COPY tpcch.district FROM '",
        "COPY tpcch.customer FROM '",
        "COPY tpcch.history FROM '",
        "COPY tpcch.neworder FROM '",
        "COPY tpcch.order FROM '",
        "COPY tpcch.orderline FROM '",
        "COPY tpcch.item FROM '",
        "COPY tpcch.stock FROM '",
        "COPY tpcch.nation FROM '",
        "COPY tpcch.supplier FROM '",
        "COPY tpcch.region FROM '"
    };

    std::vector<const char*> importSuffixStrings = {
        "/warehouse.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)",
        "/district.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)",
        "/customer.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)",
        "/history.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)",
        "/neworder.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)",
        "/order.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false, FORCE_NULL (o_carrier_id))",
        "/orderline.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false, FORCE_NULL (ol_delivery_d))",
        "/item.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)",
        "/stock.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)",
        "/nation.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)",
        "/supplier.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)",
        "/region.tbl' WITH (FORMAT CSV, ENCODING 'UTF-8', DELIMITER '|', HEADER false)"
    };

    std::vector<const char*> tpchQueryStrings = {
        // TPC-H-Query 1
        "select\n"
        "	ol_number,\n"
        "	sum(ol_quantity) as sum_qty,\n"
        "	sum(ol_amount) as sum_amount,\n"
        "	avg(ol_quantity) as avg_qty,"
        "	avg(ol_amount) as avg_amount,\n"
        "	count(*) as count_order\n"
        "from\n"
        "	tpcch.orderline\n"
        "where\n"
        "	ol_delivery_d > '2007-01-02 00:00:00.000000'\n"
        "group by\n"
        "	ol_number\n"
        "order by\n"
        "	ol_number",

        // TPC-H-Query 2
        "select\n"
        "	su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment\n"
        "from\n"
        "	tpcch.item, tpcch.supplier, tpcch.stock, tpcch.nation, tpcch.region,\n"
        "	(	select\n"
        "			s_i_id as m_i_id,\n"
        " 			min(s_quantity) as m_s_quantity\n"
        "		from\n"
        "			tpcch.stock, tpcch.supplier, tpcch.nation, tpcch.region\n"
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

        // TPC-H-Query 3
        "select\n"
        "	ol_o_id, ol_w_id, ol_d_id,\n"
        "	sum(ol_amount) as revenue, o_entry_d\n"
        "from\n"
        "	tpcch.customer, tpcch.neworder, tpcch.order, tpcch.orderline\n"
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

        // TPC-H-Query 4
        "select\n"
        "	o_ol_cnt, count(*) as order_count\n"
        "from\n"
        "	tpcch.order\n"
        "where\n"
        "		o_entry_d >= '2007-01-02 00:00:00.000000'\n"
        "	and o_entry_d < '2012-01-02 00:00:00.000000'\n"
        "	and exists \n"
        "		(	select *\n"
        "			from tpcch.orderline\n"
        "			where 	o_id = ol_o_id\n"
        "	    		and o_w_id = ol_w_id\n"
        "	    		and o_d_id = ol_d_id\n"
        "	    		and ol_delivery_d >= o_entry_d)\n"
        "group by\n"
        "	o_ol_cnt\n"
        "order by\n"
        "	o_ol_cnt",

        // TPC-H-Query 5
        "select\n"
        "	n_name,\n"
        "	sum(ol_amount) as revenue\n"
        "from\n"
        "	tpcch.customer, tpcch.order, tpcch.orderline, tpcch.stock, tpcch.supplier, tpcch.nation, tpcch.region\n"
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

        // TPC-H-Query 6
        "select\n"
        "	sum(ol_amount) as revenue\n"
        "from\n"
        "	tpcch.orderline\n"
        "where\n"
        "		ol_delivery_d >= '1999-01-01 00:00:00.000000'\n"
        "	and ol_delivery_d < '2020-01-01 00:00:00.000000'\n"
        "	and ol_quantity between 1 and 100000",

        // TPC-H-Query 7
        "select\n"
        "	su_nationkey as supp_nation,\n"
        "	substr(c_state,1,1) as cust_nation,\n"
        "	extract(year from o_entry_d) as l_year,\n"
        "	sum(ol_amount) as revenue\n"
        "from\n"
        "	tpcch.supplier, tpcch.stock, tpcch.orderline, tpcch.order, tpcch.customer, tpcch.nation n1, tpcch.nation n2\n"
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

        // TPC-H-Query 8
        "select\n"
        "	extract(year from o_entry_d) as l_year,\n"
        "	sum(case when n2.n_name = 'GERMANY' then ol_amount else 0 end) / CASE WHEN sum(ol_amount) = 0 THEN 1 ELSE sum(ol_amount) END as mkt_share\n"
        "from\n"
        "	tpcch.item, tpcch.supplier, tpcch.stock, tpcch.orderline, tpcch.order, tpcch.customer, tpcch.nation n1, tpcch.nation n2, tpcch.region\n"
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

        // TPC-H-Query 9
        "select\n"
        "	n_name, extract(year from o_entry_d) as l_year, sum(ol_amount) as sum_profit\n"
        "from\n"
        "	tpcch.item, tpcch.stock, tpcch.supplier, tpcch.orderline, tpcch.order, tpcch.nation\n"
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

        // TPC-H-Query 10
        "select\n"
        "	c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name\n"
        "from\n"
        "	tpcch.customer, tpcch.order, tpcch.orderline, tpcch.nation\n"
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

        // TPC-H-Query 11
        "select\n"
        "	s_i_id, sum(s_order_cnt) as ordercount\n"
        "from\n"
        "	tpcch.stock, tpcch.supplier, tpcch.nation\n"
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
        "			tpcch.stock, tpcch.supplier, tpcch.nation\n"
        "		where\n"
        "				s_su_suppkey = su_suppkey\n"
        "			and su_nationkey = n_nationkey\n"
        "			and n_name = 'GERMANY')\n"
        "order by\n"
        "	ordercount desc",

        // TPC-H-Query 12
        "select\n"
        "	o_ol_cnt,\n"
        "	sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,\n"
        "	sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count\n"
        "from\n"
        "	tpcch.order, tpcch.orderline\n"
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

        // TPC-H-Query 13
        "select\n"
        "	c_count, count(*) as custdist\n"
        "from\n"
        "	(	select\n"
        "			c_id, count(o_id) as c_count\n"
        "		from\n"
        "			tpcch.customer left outer join tpcch.order on (\n"
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

        // TPC-H-Query 14
        "select\n"
        "	100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1+sum(ol_amount)) as promo_revenue\n"
        "from\n"
        "	tpcch.orderline, tpcch.item\n"
        "where\n"
        "		ol_i_id = i_id\n"
        "	and ol_delivery_d >= '2007-01-02 00:00:00.000000'\n"
        "	and ol_delivery_d < '2020-01-02 00:00:00.000000'",

        // TPC-H-Query 15
        "select\n"
        "	su_suppkey, su_name, su_address, su_phone, total_revenue\n"
        "from\n"
        "	tpcch.supplier,\n"
        "		(select\n"
        "			s_su_suppkey as supplier_no,\n"
        "			sum(ol_amount) as total_revenue\n"
        "	 	from\n"
        "	 		tpcch.orderline, tpcch.stock\n"
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
        "	 			tpcch.orderline, tpcch.stock\n"
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

        // TPC-H-Query 16
        "select\n"
        "	i_name,\n"
        "	substr(i_data, 1, 3) as brand,\n"
        "	i_price,\n"
        "	count(distinct s_su_suppkey) as supplier_cnt\n"
        "from\n"
        "	tpcch.stock, tpcch.item\n"
        "where\n"
        "		i_id = s_i_id\n"
        "	and i_data not like 'zz%'\n"
        "	and (s_su_suppkey not in\n"
        "		(	select\n"
        "				su_suppkey\n"
        "		 	from\n"
        "		 		tpcch.supplier\n"
        "		 	where\n"
        "		 su_comment like '%bad%')\n"
        "		)\n"
        "group by\n"
        "	i_name, substr(i_data, 1, 3), i_price\n"
        "order by\n"
        "	supplier_cnt desc",

        // TPC-H-Query 17
        "select\n"
        "	sum(ol_amount) / 2.0 as avg_yearly\n"
        "from\n"
        "	tpcch.orderline,\n"
        "	(	select\n"
        "			i_id, avg(ol_quantity) as a\n"
        "		from\n"
        "			tpcch.item, tpcch.orderline\n"
        "		    where\n"
        "		    		i_data like '%b'\n"
        "				and ol_i_id = i_id\n"
        "		    group by\n"
        "		    	i_id\n"
        "	) t\n"
        "where\n"
        "		ol_i_id = t.i_id\n"
        "	and ol_quantity < t.a",

        // TPC-H-Query 18
        "select\n"
        "	c_last, c_id, o_id, o_entry_d, o_ol_cnt, sum(ol_amount)\n"
        "from\n"
        "	tpcch.customer, tpcch.order, tpcch.orderline\n"
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

        // TPC-H-Query 19
        "select\n"
        "	sum(ol_amount) as revenue\n"
        "from\n"
        "	tpcch.orderline, tpcch.item\n"
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

        // TPC-H-Query 20
        "select	 su_name, su_address\n"
        "from	 tpcch.supplier, tpcch.nation\n"
        "where	 su_suppkey in\n"
        "		(select  mod(s_i_id * s_w_id, 10000)\n"
        "		from     tpcch.stock, tpcch.orderline\n"
        "		where    s_i_id in\n"
        "				(select i_id\n"
        "				 from tpcch.item\n"
        "				 where i_data like 'co%')\n"
        "			 and ol_i_id=s_i_id\n"
        "			 and ol_delivery_d > '2010-05-23 12:00:00'\n"
        "		group by s_i_id, s_w_id, s_quantity\n"
        "		having   2*s_quantity > sum(ol_quantity))\n"
        "	 and su_nationkey = n_nationkey\n"
        "	 and n_name = 'GERMANY'\n"
        "order by su_name",

        // TPC-H-Query 21
        "select\n"
        "	su_name, count(*) as numwait\n"
        "from\n"
        "	tpcch.supplier, tpcch.orderline l1, tpcch.order, tpcch.stock, tpcch.nation\n"
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
        "			tpcch.orderline l2\n"
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

        // TPC-H-Query 22
        "select\n"
        "	substr(c_state,1,1) as country,\n"
        "	count(*) as numcust,\n"
        "	sum(c_balance) as totacctbal\n"
        "from\n"
        "	tpcch.customer\n"
        "where\n"
        "		substr(c_phone,1,1) in ('1','2','3','4','5','6','7')\n"
        "	and c_balance > (\n"
        "		select\n"
        "			avg(c_BALANCE)\n"
        "		from\n"
        "			tpcch.customer\n"
        "		where\n"
        "				c_balance > 0.00\n"
        "			and substr(c_phone,1,1) in ('1','2','3','4','5','6','7')\n"
        "	)\n"
        "	and not exists (\n"
        "		select *\n"
        "		from\n"
        "			tpcch.order\n"
        "		where\n"
        "				o_c_id = c_id\n"
        "			and o_w_id = c_w_id\n"
        "			and o_d_id = c_d_id\n"
        "	)\n"
        "group by\n"
        "	substr(c_state,1,1)\n"
        "order by\n"
        "	substr(c_state,1,1)"};

  public:
    // Strings to create initial database
    virtual std::vector<const char*>& getDropExistingSchemaStatements() {
        return dropExistingSchemaStatements;
    }

    virtual std::vector<const char*>& getCreateSchemaStatements() {
        return createSchemaStatements;
    }

    virtual std::vector<const char*>& getImportPrefix() {
        return importPrefixStrings;
    }

    virtual std::vector<const char*>& getImportSuffix() {
        return importSuffixStrings;
    }

    virtual std::vector<const char*>& getAdditionalPreparationStatements() {
        return additionalPreparationStatements;
    }

    // 22 adjusted TPC-H OLAP query strings
    virtual std::vector<const char*>& getTpchQueryStrings() {
        return tpchQueryStrings;
    }

    // Strings for database check
    virtual const char* getSelectCountWarehouse() {
        return "select count(*) from tpcch.warehouse";
    }

    virtual const char* getSelectCountDistrict() {
        return "select count(*) from tpcch.district";
    }

    virtual const char* getSelectCountCustomer() {
        return "select count(*) from tpcch.customer";
    }

    virtual const char* getSelectCountOrder() {
        return "select count(*) from tpcch.order";
    }

    virtual const char* getSelectCountOrderline() {
        return "select count(*) from tpcch.orderline";
    }

    virtual const char* getSelectCountNeworder() {
        return "select count(*) from tpcch.neworder";
    }

    virtual const char* getSelectCountHistory() {
        return "select count(*) from tpcch.history";
    }

    virtual const char* getSelectCountStock() {
        return "select count(*) from tpcch.stock";
    }

    virtual const char* getSelectCountItem() {
        return "select count(*) from tpcch.item";
    }

    virtual const char* getSelectCountSupplier() {
        return "select count(*) from tpcch.supplier";
    }

    virtual const char* getSelectCountNation() {
        return "select count(*) from tpcch.nation";
    }

    virtual const char* getSelectCountRegion() {
        return "select count(*) from tpcch.region";
    }

    // TPC-C transaction strings
    // NewOrder:
    virtual const char* getNoWarehouseSelect() {
        return "select W_TAX from tpcch.warehouse where W_ID=?";
    }

    virtual const char* getNoDistrictSelect() {
        return "select D_TAX, D_NEXT_O_ID from tpcch.district where D_W_ID=? and D_ID=?";
    }

    virtual const char* getNoDistrictUpdate() {
        return "update tpcch.district set D_NEXT_O_ID=D_NEXT_O_ID+1 where D_W_ID=? and D_ID=?";
    }

    virtual const char* getNoCustomerSelect() {
        return "select C_DISCOUNT,C_LAST,C_CREDIT from tpcch.customer where C_W_ID=? and C_D_ID=? and C_ID=?";
    }

    virtual const char* getNoOrderInsert() {
        return "insert into tpcch.order values (?,?,?,?,?,NULL,?,?)";
    }

    virtual const char* getNoNewOrderInsert() {
        return "insert into tpcch.neworder values(?,?,?)";
    }

    virtual const char* getNoItemSelect() {
        return "select I_PRICE,I_NAME,I_DATA from tpcch.item where I_ID=?";
    }

    virtual const char* getNoStockSelect01() {
        return "select S_QUANTITY,S_DIST_01,S_DATA from tpcch.stock where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockSelect02() {
        return "select S_QUANTITY,S_DIST_02,S_DATA from tpcch.stock where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockSelect03() {
        return "select S_QUANTITY,S_DIST_03,S_DATA from tpcch.stock where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockSelect04() {
        return "select S_QUANTITY,S_DIST_04,S_DATA from tpcch.stock where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockSelect05() {
        return "select S_QUANTITY,S_DIST_05,S_DATA from tpcch.stock where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockSelect06() {
        return "select S_QUANTITY,S_DIST_06,S_DATA from tpcch.stock where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockSelect07() {
        return "select S_QUANTITY,S_DIST_07,S_DATA from tpcch.stock where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockSelect08() {
        return "select S_QUANTITY,S_DIST_08,S_DATA from tpcch.stock where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockSelect09() {
        return "select S_QUANTITY,S_DIST_09,S_DATA from tpcch.stock where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockSelect10() {
        return "select S_QUANTITY,S_DIST_10,S_DATA from tpcch.stock where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockUpdate01() {
        return "update tpcch.stock set S_YTD=S_YTD+?, S_ORDER_CNT=S_ORDER_CNT+1, S_QUANTITY=? where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoStockUpdate02() {
        return "update tpcch.stock set S_YTD=S_YTD+?, S_ORDER_CNT=S_ORDER_CNT+1, S_QUANTITY=?, S_REMOTE_CNT=S_REMOTE_CNT+1 where S_I_ID=? and S_W_ID=?";
    }

    virtual const char* getNoOrderlineInsert() {
        return "insert into tpcch.orderline values (?,?,?,?,?,?,NULL,?,?,?)";
    }

    // Payment:
    virtual const char* getPmWarehouseSelect() {
        return "select W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP from tpcch.warehouse where W_ID=?";
    }

    virtual const char* getPmWarehouseUpdate() {
        return "update tpcch.warehouse set W_YTD=W_YTD+? where W_ID=?";
    }

    virtual const char* getPmDistrictSelect() {
        return "select D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP from tpcch.district where D_W_ID=? and D_ID=?";
    }

    virtual const char* getPmDistrictUpdate() {
        return "update tpcch.district set D_YTD=D_YTD+? where D_W_ID=? and D_ID=?";
    }

    virtual const char* getPmCustomerSelect1() {
        return "select count(*) from tpcch.customer where C_LAST=? and C_D_ID=? and C_W_ID=?";
    }

    virtual const char* getPmCustomerSelect2() {
        return "select C_ID, C_FIRST, C_MIDDLE, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE from tpcch.customer where C_LAST=? and C_D_ID=? and C_W_ID=? order by C_FIRST asc";
    }

    virtual const char* getPmCustomerSelect3() {
        return "select C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE from tpcch.customer where C_ID=? and C_D_ID=? and C_W_ID=?";
    }

    virtual const char* getPmCustomerUpdate1() {
        return "update tpcch.customer set C_BALANCE=C_BALANCE-?, C_YTD_PAYMENT=C_YTD_PAYMENT+?, C_PAYMENT_CNT=C_PAYMENT_CNT+1 where C_ID=? and C_D_ID=? and C_W_ID=?";
    }

    virtual const char* getPmCustomerSelect4() {
        return "select C_DATA from tpcch.customer where C_ID=? and C_D_ID=? and C_W_ID=?";
    }

    virtual const char* getPmCustomerUpdate2() {
        return "update tpcch.customer set C_DATA=? where C_ID=? and C_D_ID=? and C_W_ID=?";
    }

    virtual const char* getPmHistoryInsert() {
        return "insert into tpcch.history values (?,?,?,?,?,?,?,?)";
    }

    // OrderStatus:
    virtual const char* getOsCustomerSelect1() {
        return "select count(*) from tpcch.customer where C_LAST=? and C_D_ID=? and C_W_ID=?";
    }

    virtual const char* getOsCustomerSelect2() {
        return "select C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST from tpcch.customer where C_LAST=? and C_D_ID=? and C_W_ID=? order by C_FIRST asc";
    }

    virtual const char* getOsCustomerSelect3() {
        return "select C_BALANCE, C_FIRST, C_MIDDLE, C_LAST from tpcch.customer where C_ID=? and C_D_ID=? and C_W_ID=?";
    }

    virtual const char* getOsOrderSelect() {
        return "select O_ID, O_ENTRY_D, O_CARRIER_ID from tpcch.order where O_W_ID=? and O_D_ID=? and O_C_ID=? and O_ID=(select max(O_ID) from tpcch.order where O_W_ID=? and O_D_ID=? and O_C_ID=?)";
    }

    virtual const char* getOsOrderlineSelect() {
        return "select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from tpcch.orderline where OL_W_ID=? and OL_D_ID=? and OL_O_ID=?";
    }

    // Delivery:
    virtual const char* getDlNewOrderSelect() {
        return "select NO_O_ID from tpcch.neworder where NO_W_ID=? and NO_D_ID=? and NO_O_ID=(select min(NO_O_ID) from tpcch.neworder where NO_W_ID=? and NO_D_ID=?)";
    }

    virtual const char* getDlNewOrderDelete() {
        return "delete from tpcch.neworder where NO_W_ID=? and NO_D_ID=? and NO_O_ID=?";
    }

    virtual const char* getDlOrderSelect() {
        return "select O_C_ID from tpcch.order where O_W_ID=? and O_D_ID=? and O_ID=?";
    }

    virtual const char* getDlOrderUpdate() {
        return "update tpcch.order set O_CARRIER_ID=? where O_W_ID=? and O_D_ID=? and O_ID=?";
    }

    virtual const char* getDlOrderlineUpdate() {
        return "update tpcch.orderline set OL_DELIVERY_D=? where OL_W_ID=? and OL_D_ID=? and OL_O_ID=?";
    }

    virtual const char* getDlOrderlineSelect() {
        return "select sum(OL_AMOUNT) from tpcch.orderline where OL_W_ID=? and OL_D_ID=? and OL_O_ID=?";
    }

    virtual const char* getDlCustomerUpdate() {
        return "update tpcch.customer set C_BALANCE=C_BALANCE+?, C_DELIVERY_CNT=C_DELIVERY_CNT+1 where C_ID=? and C_D_ID=? and C_W_ID=?";
    }

    // StockLevel:
    virtual const char* getSlDistrictSelect() {
        return "select D_NEXT_O_ID from tpcch.district where D_W_ID=? and D_ID=?";
    }

    virtual const char* getSlStockSelect() {
        return "select count(*) from tpcch.stock,(select distinct OL_I_ID from tpcch.orderline where OL_W_ID=? and OL_D_ID=? and OL_O_ID<? and OL_O_ID>=?) _ where S_I_ID=OL_I_ID and S_W_ID=? and S_QUANTITY<?";
    }
};

#endif
