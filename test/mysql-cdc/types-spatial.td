# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=1s
$ set-max-tries max-tries=20


#
# Spatial types not supported
#

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE geometry_type (f1 GEOMETRY);
INSERT INTO geometry_type VALUES (POINT(48.290003372145726, 14.291629908347193));

CREATE TABLE geometry_type_with_srid (f1 GEOMETRY NOT NULL SRID 4326);
INSERT INTO geometry_type_with_srid VALUES (ST_SRID(POINT(51.50556752713903, -0.07534114282176071), 4326));

CREATE TABLE point_type (f1 POINT);
INSERT INTO point_type VALUES (ST_GeomFromText('POINT(69.91075849296699 27.03176710555435)'));

CREATE TABLE linestring_type (f1 LINESTRING);
INSERT INTO linestring_type VALUES (ST_GeomFromText('LINESTRING(0 0,1 1,2 2)'));

CREATE TABLE polygon_type (f1 POLYGON);
INSERT INTO polygon_type VALUES (ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))'));

CREATE TABLE multipoint_type (f1 MULTIPOINT);
INSERT INTO multipoint_type VALUES (ST_GeomFromText('MULTIPOINT(0 0, 20 20, 80 80, 120 120)'));

CREATE TABLE multilinestring_type (f1 MULTILINESTRING);
INSERT INTO multilinestring_type VALUES (ST_GeomFromText('MULTILINESTRING((10 10, 20 20), (15 15, 60 15))'));

CREATE TABLE multipolygon_type (f1 MULTIPOLYGON);
INSERT INTO multipolygon_type VALUES (ST_GeomFromText('MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((8 8,7 8,7 7,8 7, 8 8)))'));

CREATE TABLE geometrycollection_type (f1 GEOMETRYCOLLECTION);
INSERT INTO geometrycollection_type VALUES (ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))'));


! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.geometry_type);
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.geometry_type_with_srid);
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.point_type);
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.linestring_type);
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.polygon_type);
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.multipoint_type);
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.multilinestring_type);
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.multipolygon_type);
contains:referenced tables use unsupported types

! CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_conn
  FOR TABLES (public.geometrycollection_type);
contains:referenced tables use unsupported types
