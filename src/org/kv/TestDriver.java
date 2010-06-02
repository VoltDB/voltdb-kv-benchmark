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

package org.kv;

import junit.framework.TestCase;

public class TestDriver extends TestCase {

    KVConfig getBaseConfig() {
        KVConfig config = new KVConfig();
        config.valueSizeInBytes = 12000;
        config.initialSize = 10000;
        config.getsFraction = 0.5;
        //config.benchmarkLevel = KVConfig.INTS_BENCH;
        config.benchmarkLevel = KVConfig.BATCH_BENCH;
        config.waitForKeypress = 0;
        config.isLoader = 1;
        return config;
    }

    /*public void testVoltKV() {
        VoltConnection.main(new String[] { "6" });

        VoltDB.Configuration voltConfig = new VoltDB.Configuration();
        voltConfig.m_backend = BackendTarget.NATIVE_EE_JNI;
        voltConfig.m_pathToCatalog = "kv.jar";
        ServerThread server = new ServerThread(voltConfig);
        server.start();
        server.waitForInitialization();

        KVConfig config = getBaseConfig();
        config.storageType = "VOLTDB";
        config.connectionCount = 8;

        Driver driver = new Driver(config);
        driver.runBenchmark();

        try {
            server.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }*/

    public void testMemcacheKV() {
        KVConfig config = getBaseConfig();
        config.storageType = "MYSQL";
        config.connectionCount = 4;
        //config.mysqlUseMemcache = 1;

        Driver driver = new Driver(config);
        driver.runBenchmark();
    }

    /*public void testCassandra() {
        KVConfig config = getBaseConfig();
        //config.serverHosts = new String[] { "volt3a", "volt3b", "volt3c" };
        config.serverHosts = new String[] { "volt3a" };
        config.storageType = "CASSANDRA";
        config.connectionCount = 64;

        config.cassReadConsistencyLevel = "ONE";
        config.cassWriteConsistencyLevel = "ANY";

        Driver driver = new Driver(config);
        driver.runBenchmark();
    }*/
}
