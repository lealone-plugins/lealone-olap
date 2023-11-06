/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.test;

import java.sql.ResultSet;

import org.junit.Test;
import org.lealone.db.LealoneDatabase;
import org.lealone.test.sql.SqlTestBase;

public class OlapTest extends SqlTestBase {

    public OlapTest() {
        super(LealoneDatabase.NAME);
        setEmbedded(true);
    }

    @Test
    public void crud() throws Exception {
        executeUpdate("DROP TABLE IF EXISTS test");
        String sql = "CREATE TABLE IF NOT EXISTS test (f1 int primary key, f2 long)";
        executeUpdate(sql);

        executeUpdate("INSERT INTO test(f1, f2) VALUES(1, 1)");
        executeUpdate("UPDATE test SET f2 = 2 WHERE f1 = 1");
        ResultSet rs = stmt.executeQuery("SELECT * FROM test");
        assertTrue(rs.next());
        System.out.println("f1=" + rs.getInt(1) + " f2=" + rs.getLong(2));
        assertFalse(rs.next());
        rs.close();
        executeUpdate("DELETE FROM test WHERE f1 = 1");
        rs = stmt.executeQuery("SELECT * FROM test");
        assertFalse(rs.next());
        rs.close();

        // 测试olap
        batchInsert();
        executeUpdate("set olap_threshold 100");
        rs = stmt.executeQuery("SELECT * FROM test");
        rs.close();
    }

    public void batchInsert() throws Exception {
        for (int i = 1; i <= 600; i++)
            executeUpdate("INSERT INTO test(f1, f2) VALUES(" + i + ", " + i * 10 + ")");
    }
}
