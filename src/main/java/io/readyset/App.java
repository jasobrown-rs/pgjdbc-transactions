package io.readyset;

import java.sql.*;
import java.util.*;

public class App {
    static boolean usePg;
    
    // NOTE: `conn.setAutoCommit(false)` is the way to for the JDBC driver
    // to BEGIN a transaction. It will start on the first statment you pass
    // to the connection. Further, you never need to actually issue a "BEGIN"
    // yourself (either through simple or extended query protocols).
    private enum Action {
        NO_TRANSACTION,
        AUTO_COMMIT,
    }
    
    public static void main(String[] args) throws Exception {
        schemaSetup(getConn());

        // doPreparedStatmentTests(getConn());
        
        // // // this should succeed, as no transaction is created.
        // doSimpleSelect(getConn(), Action.NO_TRANSACTION);

        // // // this should fail. pgjdbc will see that it needs to (automagically)
        // // // being a transaction, so it sends a BEGIN statement using extended protocol query.
        // doSimpleSelect(getConn(), Action.AUTO_COMMIT);

        doUpdate(getConn(), false);
        doUpdate(getConn(), true);

        // doInsertAndDelete(getConn(), Action.NO_TRANSACTION);
        // doInsertAndDelete(getConn(), Action.AUTO_COMMIT);

        // doNoDataRead(getConn());

        // batched inserted
        // doBatchedInserts(getConn(false));
        // doBatchedInserts(getConn(true));
    }

    private static Connection getConn() throws Exception {
        return getConn(false);
    }

    private static Connection getConn(boolean doMultiRowDml) throws Exception {
        // Actually, mysql doesn't quite work here as we don't know wtf it's
        // pStmt ids are, and there's no view or tooling to know them.
        String db = System.getenv("DB");
        String port = System.getenv("PORT");

        Connection conn;
        if (db.equals("postgres")) {
            usePg = true;
            String url = "jdbc:postgresql://localhost:" + port + "/testdb";
            Properties props = new Properties();
            props.setProperty("user", "postgres");
            props.setProperty("password", "readyset");
            props.setProperty("ssl", "false");
            props.setProperty("prepareThreshold", "-1");
            //props.setProperty("preparedStatementCacheQueries", "1");

            if (doMultiRowDml) {
                props.setProperty("reWriteBatchedInserts", "true");
            }
            
            conn = DriverManager.getConnection(url, props);
        } else if (db.equals("mysql")) {
            Class.forName("com.mysql.cj.jdbc.Driver");
            String url = "jdbc:mysql://localhost:" + port + "/testdb";
            Properties props = new Properties();
            props.setProperty("user", "root");
            props.setProperty("password", "readyset");
            props.setProperty("sslMode", "DISABLED");
            props.setProperty("useServerPrepStmts", "true");
            props.setProperty("emulateUnsupportedPstmts", "false");
            conn = DriverManager.getConnection(url, props);
        } else {
            throw new IllegalArgumentException("unknown db: " + db);
        }

        // disable "automatic transactions" :facepalm:
        conn.setAutoCommit(true);
        return conn;
    }

    private static void schemaSetup(Connection conn) throws SQLException {
        Statement st = conn.createStatement();
        st.execute("drop table if exists dogs");
        st.execute("create table dogs (id int, name varchar(64), birth_date timestamp default CURRENT_TIMESTAMP)");

        st.execute("insert into dogs values(1, 'kidnap')");
    }
    
    private static void doPreparedStatmentTests(Connection conn) {
        System.out.println("********** doPreparedStatmentTests() ***********");
        try {
            PreparedStatement[] stmts = new PreparedStatement[]{
                conn.prepareStatement("select name from dogs where id = ?"),
                conn.prepareStatement("select * from dogs where id = ?")
            };
            
            // Test 1 - create, execute, and close a pStmt
            // need to do this twice as the close will not get sent unless another PG message is sent.
            for (int i = 0; i < stmts.length * 100; i++) {
                PreparedStatement st = stmts[i % stmts.length];
                st.setInt(1, 1);
                ResultSet rs = st.executeQuery();
                while (rs.next()) {
                    //System.out.println(rs.getString(1));                    
                }
                rs.close();
            }

            if (!usePg) {
                for (int i = 0; i < stmts.length; i++) {
                    stmts[i].close();
                }
                return;
            }

            // Test 2 - create, execute, and deallocate a pStmt
            PreparedStatement ps = stmts[0];
            ps.setInt(1, 1);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
            }
            rs.close();

            System.out.println("*** after initial pStmt setup ***");
            Statement st = conn.createStatement();
            rs = st.executeQuery("SELECT * FROM pg_prepared_statements");
            String deallocId = "BAD_ID";
            while (rs.next()) {
                String id = rs.getString(1);
                String stmt = rs.getString(2);
                // holy shit, PG, at least return them in order :facepalm:
                if (!stmt.contains("pg_prepared_statements")) {
                    deallocId = id;
                }
                System.out.println(id + " : " + stmt);
            }
            rs.close();
            st.close();

            // dealloc single
            // NOTE: this may error when connections directly to PG. For some reason,
            // PG is lower casing the stmt id even though the driver is sendin upper case.
            // for example: driver sends "S_1", but the error from PG is
            // 'ERROR: prepared statement "s_1" does not exist'. This might be only related to when
            // the DEALLOCATE statement is sent over the extended qquery protocol, but wtf?!?!
            System.out.println("*** about to dealloc " + deallocId + " ***");
            st = conn.createStatement();
            st.execute("DEALLOCATE " + deallocId);
            st.close();

            System.out.println("*** after dealloc single (" + deallocId +") ***");
            st = conn.createStatement();
            rs = st.executeQuery("SELECT * FROM pg_prepared_statements");
            while (rs.next()) {
                System.out.println(rs.getString(1) + " : " + rs.getString(2));
            }
            rs.close();
            st.close();

            // dealloc all
            System.out.println("*** about to dealloc all ***");
            st = conn.createStatement();
            st.execute("DEALLOCATE all");
            st.close();

            System.out.println("*** after dealloc all ***");
            st = conn.createStatement();
            rs = st.executeQuery("SELECT * FROM pg_prepared_statements");
            while (rs.next()) {
                System.out.println(rs.getString(1) + " : " + rs.getString(2));
            }
            rs.close();
            st.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch(SQLException e) {
                // NOP
            }
        }
    }
    
    private static void doSimpleSelect(Connection conn, Action action) {
        System.out.println("********** doSimpleSelect(), action: " + action + " ***********");
        try {
            switch (action) {
                case NO_TRANSACTION:
                    conn.setAutoCommit(true); 
                    break;
                case AUTO_COMMIT:
                    conn.setAutoCommit(false);
                    break;
            }

            PreparedStatement st = conn.prepareStatement("select name from dogs where id = ?");
            st.setInt(1, 1);

            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                System.out.print("Column 1 returned: ");
                System.out.println(rs.getString(1));
            }
            rs.close();
            st.close();

            switch (action) {
                case NO_TRANSACTION:
                    // nop
                    break;
                case AUTO_COMMIT:
                    conn.commit();
                    break;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch(SQLException e) {
                // NOP
            }
        }
    }

    private static void doUpdate(Connection conn, boolean updateTimestamp) {
        System.out.println("********** doUpdate(updateTimestamp: " + updateTimestamp +") ***********");
        try {
            PreparedStatement st;
            if (updateTimestamp) {
                st = conn.prepareStatement("update dogs set name = ?, birth_date = ? where id = ?");
                st.setString(1, "fido");
                st.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                st.setInt(3, 1);
            } else {
                st = conn.prepareStatement("update dogs set name = ?  where id = ?");
                st.setString(1, "fido");
                st.setInt(2, 1);
            }

            int cnt = st.executeUpdate();
            st.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch(SQLException e) {
                // NOP
            }            
        }
    }

    private static void doInsertAndDelete(Connection conn, Action action) {
        System.out.println("********** doInsertAndDelete(), action: " + action + " ***********");
        final int id = 71234133;
        try {
            switch (action) {
                case NO_TRANSACTION:
                    conn.setAutoCommit(true); 
                    break;
                case AUTO_COMMIT:
                    conn.setAutoCommit(false);
                    break;
            }

            PreparedStatement st = conn.prepareStatement("insert into dogs values(?, 'rando')");
            st.setInt(1, id);
            int cnt = st.executeUpdate();
            st.close();

            st = conn.prepareStatement("delete from dogs where id = ?");
            st.setInt(1, id);
            cnt = st.executeUpdate();
            st.close();

            switch (action) {
                case NO_TRANSACTION:
                    // nop
                    break;
                case AUTO_COMMIT:
                    conn.commit();
                    break;
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch(SQLException e) {
                // NOP
            }
        }
    }

    private static void doNoDataRead(Connection conn) {
        System.out.println("********** doNoDataRead() ***********");
        final int id = 90134136;
        try {
            PreparedStatement st = conn.prepareStatement("select name from dogs where id = ?");
            st.setInt(1, id);

            ResultSet rs = st.executeQuery();
            while (rs.next()) {
                System.out.print("Column 1 returned: ");
                System.out.println(rs.getString(1));
            }
            rs.close();
            st.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch(SQLException e) {
                // NOP
            }            
        }
    }

    private static void doBatchedInserts(Connection conn) {
        System.out.println("********** doBatchedInserts() ***********");
        final int baseId = 581800;
        final int insertCount = 4;
        try {
            PreparedStatement st = conn.prepareStatement("insert into dogs values(?, ?, ?)");

            for (int i = 0; i < insertCount; i++) {
                int id = baseId + i;
                Timestamp ts = new Timestamp(System.currentTimeMillis());
                st.setInt(1, id);
                st.setString(2, "t_" + id);
                st.setTimestamp(3, ts);
                st.addBatch();
            }
            int cnts[] = st.executeBatch();
            if (cnts.length != insertCount) {
                System.out.println("different results cnt vs insert stmts: " + cnts.length + " / " + insertCount);
            }

            for (int i = 0; i < cnts.length; i++) {
                if (!(cnts[i] == 1 || cnts[i] == Statement.SUCCESS_NO_INFO)) {
                    System.out.println("result of insert[" + i  + "] != 1: " + cnts[i]);
                }
            }
            
            st.clearBatch();
            st.close();
        } catch (SQLException ex) {
            ex.printStackTrace();
            try {
                conn.rollback();
            } catch(SQLException e) {
                // NOP
            }            
        }
    }
}
