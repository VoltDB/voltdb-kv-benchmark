/* Copyright (C) 2010 VoltDB L.L.C.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.kv.myql;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.kv.KVConfig;
import org.kv.RuntimeStats;
import org.kv.StorageLayerConnection;

import com.danga.MemCached.MemCachedClient;
import com.danga.MemCached.SockIOPool;

public class MySQLConnection extends StorageLayerConnection {

	// sql for benchmark 1
	static final String insertBlobSQL      = "INSERT INTO `blobdata` (`keycolumn`, `valuecolumn`) VALUES (?, ?);";
	static final String updateBlobSQL      = "UPDATE `blobdata` SET `valuecolumn` = ? WHERE `keycolumn` = ?;";
	static final String getBlobSQL         = "SELECT `keycolumn`, `valuecolumn` FROM `blobdata` WHERE `keycolumn` = ?;";

	// sql for benchmarks 2 and 3
    static final String insertIntsTemplate = "INSERT INTO `manyints` VALUES (%s?);";
    static final String updateIntsTemplate = "UPDATE `manyints` SET `col%d` = ? WHERE `keycolumn` = ?;";
    static final String getIntsTemplate    = "SELECT `col%d` FROM `manyints` WHERE `keycolumn` = ?;";
    static final String selectIntsSql      = "SELECT * FROM `manyints` WHERE `keycolumn` = ?;";
    static final String callIntsTemplate   = "CALL manyintsop%d(?, ?, ?);";

    // prepared statements for benchmark 1
    PreparedStatement stmtBlobInsert;
    PreparedStatement stmtBlobUpdate;
    PreparedStatement stmtBlobGet;

    // prepared statements for benchmarks 2 and 3
    PreparedStatement stmtIntsInsert;
    PreparedStatement stmtSelectInts;
    PreparedStatement[] stmtIntsUpdate = new PreparedStatement[KVConfig.INT_COLUMN_COUNT];
    PreparedStatement[] stmtIntsGet = new PreparedStatement[KVConfig.INT_COLUMN_COUNT];

    // stored proc for benchmark 2
    CallableStatement[] stmtIntsProc = new CallableStatement[KVConfig.INT_COLUMN_COUNT];

    // config
    boolean usecache = false;
    boolean useprocs = false;
    boolean commitIntsOp = true;
    boolean autoCommit = false;

    // mysql client
	Connection conn = null;
    Statement stmt = null;

    // memcache client if used
    MemCachedClient mcc = null;

	@Override
	public void close() throws Exception {
		conn.close();
	}

	@Override
	public void initializeAndConnect(KVConfig config) throws Exception {
		assert(config != null);
		assert(config.serverHosts.length == 1);
		assert(config.serverHosts[0] != null);

		// benchmark 3 just calls benchmark 2 10 times
		// don't commit 10 times if running benchmark 3
		if (config.benchmarkLevel == KVConfig.BATCH_BENCH)
			commitIntsOp = false;

		// connect to mysql
		String dbURL = String.format("jdbc:mysql://%s/kv?user=%s&password=%s",
				config.mysqlUser, config.mysqlPassword, config.serverHosts[0]);
        conn =  DriverManager.getConnection(dbURL);
        conn.setAutoCommit(autoCommit);
        stmt = conn.createStatement();

        // create the prepared stmts
        stmtBlobInsert = conn.prepareStatement(insertBlobSQL);
        stmtBlobUpdate = conn.prepareStatement(updateBlobSQL);
        stmtBlobGet = conn.prepareStatement(getBlobSQL);

        // some of this is hacky to deal with random column access
        String intsInsertMiddle = "";
        for (int i = 1; i <= KVConfig.INT_COLUMN_COUNT; i++) {
            intsInsertMiddle += "?, ";
        }
        stmtIntsInsert = conn.prepareStatement(String.format(insertIntsTemplate, intsInsertMiddle));
        stmtSelectInts = conn.prepareStatement(selectIntsSql);
        for (int i = 0; i < KVConfig.INT_COLUMN_COUNT; i++) {
            stmtIntsGet[i] = conn.prepareStatement(String.format(getIntsTemplate, i));
            stmtIntsUpdate[i] = conn.prepareStatement(String.format(updateIntsTemplate, i));
            stmtIntsProc[i] = conn.prepareCall(String.format(callIntsTemplate, i));
        }

        // init memcache
        usecache = config.mysqlUseMemcache == 1;
        if (usecache) initMemcache(config);

        // decide about procs
        useprocs = config.mysqlUseProcedures == 1;
	}

	/**
	 * Initialize a connection to memcached. This code is basically from
	 * the memcached website.
	 */
	void initMemcache(KVConfig config) {
	    mcc = new MemCachedClient();

	    // server list and weights
	    String[] servers = { config.serverHosts[0] + ":1624" };
	    Integer[] weights = { 1 };

	    // grab an instance of our connection pool
	    SockIOPool pool = SockIOPool.getInstance();

	    // set the servers and the weights
	    pool.setServers( servers );
	    pool.setWeights( weights );

	    // set some basic pool settings
	    // 5 initial, 5 min, and 250 max conns
	    // and set the max idle time for a conn
	    // to 6 hours
	    pool.setInitConn( 5 );
	    pool.setMinConn( 5 );
	    pool.setMaxConn( 250 );
	    pool.setMaxIdle( 1000 * 60 * 60 * 6 );

	    // set the sleep for the maint thread
	    // it will wake up every x seconds and
	    // maintain the pool size
	    pool.setMaintSleep( 30 );

	    // set some TCP settings
	    // disable nagle
	    // set the read timeout to 3 secs
	    // and don't set a connect timeout
	    pool.setNagle( false );
	    pool.setSocketTO( 3000 );
	    pool.setSocketConnectTO( 0 );

	    // initialize the connection pool
	    pool.initialize();
	}

	@Override
    public void queueBlobInsert(String key, byte[] value, RuntimeStats stats) throws Exception {
	    long startTime = stats.puts.noteSent();

        // MYSQL INSERTION
        stmtBlobInsert.setString(1, key);
        stmtBlobInsert.setBytes(2, value);
        int res = stmtBlobInsert.executeUpdate();
        if (res != 1) {
            SQLWarning warning = stmtBlobInsert.getWarnings();
            while (warning != null) {
                System.err.printf("SQL Warning: %s\n", warning.getMessage());
                warning = warning.getNextWarning();
            }
            System.err.printf("Put failed insert for key %s on put # %d with updated count %d\n.",
                    key, stats.puts.numSent.get(), res);
            System.exit(-1);
        }
        if (!autoCommit) conn.commit();

        // MEMCACHE INSERTION
        if (usecache) mcc.set(key, value);

	    stats.puts.noteReceived(startTime);
	}

	@Override
	public void queueInsertIntegerSet(String key, int[] values, RuntimeStats stats) throws Exception {
	    long startTime = stats.puts.noteSent();

	    assert(values.length == KVConfig.INT_COLUMN_COUNT);

	    // MYSQL INSERTION
	    stmtIntsInsert.setString(1, key);
	    for (int i = 0; i < KVConfig.INT_COLUMN_COUNT; i++) {
	        stmtIntsInsert.setInt(i + 2, values[i]);
	    }
	    int res = stmtIntsInsert.executeUpdate();
        if (res != 1) {
            SQLWarning warning = stmtIntsInsert.getWarnings();
            while (warning != null) {
                System.err.printf("SQL Warning: %s\n", warning.getMessage());
                warning = warning.getNextWarning();
            }
            System.err.printf("Put failed insert for key %s on put # %d with updated count %d\n.",
                    key, stats.puts.numSent.get(), res);
            System.exit(-1);
        }
        if (!autoCommit) conn.commit();

        // MEMCACHE INSERTION
        if (usecache) mcc.set(key, values);

	    stats.puts.noteReceived(startTime);
    }

	@Override
	public void queueBlobGet(String key, RuntimeStats stats) throws Exception {
		long startTime = stats.gets.noteSent();

		// MEMCACHE GET
		if (usecache) {
		    Object value = mcc.get(key);
		    if (value != null) {
		        stats.gets.noteReceived(startTime);
		        return;
		    }
		}

		// MYSQL GET
		stmtBlobGet.setString(1, key);
		ResultSet res = stmtBlobGet.executeQuery();
		if (!res.next()) {
			System.err.printf("Get missed for key: %s on get # %d.\n", key, stats.gets.numSent.get());
			System.exit(-1);
		}
		if (!autoCommit) conn.commit();

		stats.gets.noteReceived(startTime);
	}

	@Override
	public void queueBlobPut(String key, byte[] value, RuntimeStats stats) throws Exception {
		long startTime = stats.puts.noteSent();

		// MYSQL PUT
		stmtBlobUpdate.setBytes(1, value);
		stmtBlobUpdate.setString(2, key);
		int res = stmtBlobUpdate.executeUpdate();
		if (res != 1) {
			SQLWarning warning = stmtBlobUpdate.getWarnings();
			while (warning != null) {
				System.err.printf("SQL Warning: %s\n", warning.getMessage());
				warning = warning.getNextWarning();
			}
			System.err.printf("Put failed update for key %s on put # %d with updated count %d\n.",
					key, stats.puts.numSent.get(), res);
			System.exit(-1);
		}
		if (!autoCommit) conn.commit();

		// MEMCACHE PUT
        if (usecache) mcc.set(key, value);

		stats.puts.noteReceived(startTime);
	}

    @Override
    public void queueManyIntsOp(String key, int readIndex, int writeIndex, int randValue, RuntimeStats stats) throws Exception {
        long startTime = stats.puts.noteSent();

        if (useprocs) {
            CallableStatement proc = stmtIntsProc[readIndex];
            proc.setString(1, key);
            proc.setInt(2, writeIndex);
            proc.setInt(3, randValue);
            proc.execute();
        }
        else {
            int[] row = null;
            int decisionValue = Integer.MAX_VALUE;
            boolean stillNeedsDecisionValue = true;

            if (usecache) {
                Object rowRaw = mcc.get(key);
                if (rowRaw != null) {
                    row = (int[]) rowRaw;
                    assert(row.length == KVConfig.INT_COLUMN_COUNT);
                    decisionValue = row[readIndex];
                }
            }

            PreparedStatement get = stmtIntsGet[readIndex];
            PreparedStatement put = stmtIntsUpdate[writeIndex];

            if (stillNeedsDecisionValue) {
                get.setString(1, key);
                ResultSet res = get.executeQuery();
                if (!res.next()) {
                    System.err.printf("Get missed for key: %s on get # %d.\n", key, stats.gets.numSent.get());
                    System.exit(-1);
                }
                decisionValue = res.getInt(1);
            }

            // if number is odd, update another random column
            // with a random value
            if ((decisionValue % 2) == 1) {
                put.setString(2, key);
                put.setInt(1, randValue);
                int res2 = put.executeUpdate();
                if (res2 != 1) {
                    SQLWarning warning = put.getWarnings();
                    while (warning != null) {
                        System.err.printf("SQL Warning: %s\n", warning.getMessage());
                        warning = warning.getNextWarning();
                    }
                    System.err.printf("Put failed update for key %s on put # %d with updated count %d\n.",
                            key, stats.puts.numSent.get(), res2);
                    System.exit(-1);
                }

                if (usecache) {
                    if (row == null) {
                        row = new int[KVConfig.INT_COLUMN_COUNT];
                        ResultSet res = stmtSelectInts.executeQuery();
                        if (!res.next()) {
                            System.err.printf("Get missed for key: %s on get # %d.\n", key, stats.gets.numSent.get());
                            System.exit(-1);
                        }
                        for (int i = 0; i < KVConfig.INT_COLUMN_COUNT; i++) {
                            row[i] = res.getInt(i + 2);
                        }
                    }
                    row[writeIndex] = randValue;
                    mcc.set(key, row);
                }
            }
        }
        if (commitIntsOp)
        	if (!autoCommit) conn.commit();

        stats.puts.noteReceived(startTime);
    }

    @Override
    public void queueManyIntsOpBatched(String key, int[] readIndices, int[] writeIndices, int[] randValues, RuntimeStats stats) throws Exception {
        for (int i = 0; i < readIndices.length; i++) {
            queueManyIntsOp(key, readIndices[i], writeIndices[i], randValues[i], stats);
        }
        if (!autoCommit) conn.commit();
    }

    /**
     * Reset the schema and stored procs so we have a fresh empty database.
     */
	public static void resetData(KVConfig config) {
		assert(config != null);
		assert(config.serverHosts.length == 1);
		assert(config.serverHosts[0] != null);

		System.out.println("Beginning mysql data and schema reset.");

		String dbURL = String.format("jdbc:mysql://%s/kv?user=root&password=mysql", config.serverHosts[0]);

		try {
            Connection conn =  DriverManager.getConnection(dbURL);
            Statement stmt = conn.createStatement();

	        // drop the table if it exists
            stmt.executeUpdate("DROP TABLE IF EXISTS `blobdata`;");
            stmt.executeUpdate("DROP TABLE IF EXISTS `manyints`;");

            String blobTablePrefix = String.format("CREATE TABLE `%s` (`keycolumn` VARCHAR(250) NOT NULL, ", "blobdata");
            String intsTablePrefix = String.format("CREATE TABLE `%s` (`keycolumn` VARCHAR(250) NOT NULL, ", "manyints");

            // use the storage engine from config data
            String suffix = String.format("PRIMARY KEY (`keycolumn`)) ENGINE %s;", config.mysqlStorageEngine);

            String blobBody = "`valuecolumn` BLOB(1048576) NOT NULL, ";
            String intsBody = "";
            for (int i = 0; i < KVConfig.INT_COLUMN_COUNT; i++) {
                intsBody += String.format("`col%d` INTEGER(4) NOT NULL, ", i);
            }

	        // create the table
            stmt.executeUpdate(blobTablePrefix + blobBody + suffix);
            stmt.executeUpdate(intsTablePrefix + intsBody + suffix);

            loadStoredProcs(config, conn, stmt);

            conn.close();

            System.out.println("Finished mysql data and schema reset.");
		}
        catch (Exception ex) {
        	// fail fast
            ex.printStackTrace();
            System.exit(-1);
        }
	}

	/**
	 * Load/replace a mysql stored proc to do benchmark 2.
	 */
	public static void loadStoredProcs(KVConfig config, Connection conn, Statement stmt) throws Exception {
	    for (int i = 0; i < KVConfig.INT_COLUMN_COUNT; i++) {
	        // drop the procedures if they exist
            stmt.executeUpdate(String.format("DROP PROCEDURE IF EXISTS `manyintsop%d`;", i));

            StringBuilder sb = new StringBuilder();
            sb.append(    String.format("CREATE PROCEDURE manyintsop%d(IN keyval VARCHAR(255), IN writecolumn INT, IN randvalue INT)\n", i));
            sb.append(                  "BEGIN\n");
            sb.append(                  "    DECLARE readvalue INT;\n");
            sb.append(    String.format("    SELECT `col%d` INTO readvalue FROM `manyints` WHERE `keycolumn` = keyval;\n", i));
            sb.append(                  "    IF ((readvalue MOD 2) = 1) THEN\n");
            sb.append(                  "        CASE writecolumn\n");
            for (int j = 0; j < KVConfig.INT_COLUMN_COUNT; j++) {
                sb.append(String.format("            WHEN %d THEN\n", j));
                sb.append(String.format("                UPDATE `manyints` SET `col%d` = randvalue;\n", j));
            }
            sb.append(                  "        END CASE;\n");
            sb.append(                  "    END IF;\n");
            sb.append(                  "END");
            stmt.executeUpdate(sb.toString());
        }
    }
}
