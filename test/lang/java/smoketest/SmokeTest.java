// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

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
        String url = String.format("jdbc:postgresql://%s:%s/", host, port);
        conn = DriverManager.getConnection(url);
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
}
