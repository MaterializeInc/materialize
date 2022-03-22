// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.jdbc.TypeInfoCache;

class SmokeTest {
    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException, java.lang.ClassNotFoundException {
        String host = System.getenv("PGHOST");
        if (host == null)
            host = "localhost";
        String port = System.getenv("PGPORT");
        if (port == null)
            port = "6875";
        String url = String.format("jdbc:postgresql://%s:%s/materialize", host, port);
        conn = DriverManager.getConnection(url, "materialize", null);
    }

    @AfterEach
    void tearDown() throws SQLException, java.lang.ClassNotFoundException {
        conn.close();
    }

    @Test
    void testParamString() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("SELECT ?");
        stmt.setString(1, "foo");
        ResultSet rs = stmt.executeQuery();
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals("foo", rs.getString(1));
        rs.close();
        stmt.close();
    }

    @Test
    void testParamInt() throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("SELECT ?");
        stmt.setInt(1, 42);
        ResultSet rs = stmt.executeQuery();
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals("42", rs.getString(1));
        Assertions.assertEquals(42, rs.getInt(1));
        rs.close();
        stmt.close();
    }

    // Regression for #4117.
    @Test
    void testBinaryTimestamp() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.jdbc.PgConnection");
        conn.unwrap(org.postgresql.jdbc.PgConnection.class).setForceBinary(true);
        PreparedStatement stmt = conn.prepareStatement("SELECT '2010-01-02'::timestamp");
        ResultSet rs = stmt.executeQuery();
        Assertions.assertTrue(rs.next());
        Assertions.assertEquals("2010-01-02 00:00:00", rs.getString(1));
        rs.close();
        stmt.close();
    }

    @Test
    void testGetSqlType() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.core.BaseConnection");
        TypeInfoCache ic = new TypeInfoCache(conn.unwrap(org.postgresql.core.BaseConnection.class), 0);
        Assertions.assertEquals(ic.getSQLType("int2"), Types.SMALLINT);
        Assertions.assertEquals(ic.getSQLType("_int2"), Types.ARRAY);
    }

    @Test
    void testPgJDBCgetColumns() throws SQLException, ClassNotFoundException {
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE materialize.public.getcols (a INT, b STRING)");
        stmt.close();

        ResultSet columns = conn.getMetaData().getColumns("materialize", "public", "getcols", null);
        Assertions.assertTrue(columns.next());
        Assertions.assertEquals("a", columns.getString("COLUMN_NAME"));
        Assertions.assertTrue(columns.next());
        Assertions.assertEquals("b", columns.getString("COLUMN_NAME"));
        Assertions.assertFalse(columns.next());
        columns.close();

        stmt = conn.createStatement();
        stmt.execute("DROP TABLE materialize.public.getcols");
        stmt.close();
    }

    @Test
    void testPgJDBCgetPrimaryKeys() throws SQLException, ClassNotFoundException {
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE materialize.public.getpks (a INT, b STRING)");
        stmt.close();

        ResultSet columns = conn.getMetaData().getPrimaryKeys("materialize", "public", "getpks");
        Assertions.assertFalse(columns.next());
        columns.close();

        stmt = conn.createStatement();
        stmt.execute("DROP TABLE materialize.public.getpks");
        stmt.close();
    }
}
