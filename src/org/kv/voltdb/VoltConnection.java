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

package org.kv.voltdb;

import org.kv.KVConfig;
import org.kv.RuntimeStats;
import org.kv.StorageLayerConnection;
import org.kv.voltdb.procedures.GetBlob;
import org.kv.voltdb.procedures.InsertInts;
import org.kv.voltdb.procedures.ManyInts;
import org.kv.voltdb.procedures.ManyIntsBatched;
import org.kv.voltdb.procedures.UpdateBlob;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;

public class VoltConnection extends StorageLayerConnection {

    // static global client object instance
    // The VoltDB java client is concurrent, async and fast. There's
    //  no need to have more than one.
    static org.voltdb.client.Client globalClient = null;
    static int connections = 0;

    org.voltdb.client.Client client = null;

    /**
     * Represents a callback object used to check the success or
     * failure of a benchmark command. It also supports checking
     * latency and is responsible for updating the stats counters.
     *
     */
    class AsyncCallback implements ProcedureCallback {
        final RuntimeStats.ProcStats procStats;
        final long startTime;

        public AsyncCallback(RuntimeStats.ProcStats procStats) {
            this.procStats = procStats;
            this.startTime = procStats.noteSent();
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            final byte status = clientResponse.getStatus();

            if (status != ClientResponse.SUCCESS) {
                System.err.println("Failed to execute VoltDB stored procedure.");
                System.err.println(clientResponse.getStatusString());
                System.err.println(clientResponse.getException());
                System.exit(-1);
            }

            procStats.noteReceived(startTime);
        }
    }

    @Override
    public synchronized void initializeAndConnect(KVConfig config) throws Exception {
        // note this method is synchronized

        assert(config.serverHosts.length > 0);

        // if this is the first client thread, create the single global client
        if (globalClient == null) {
            globalClient = ClientFactory.createClient();
            for (String host : config.serverHosts) {
                assert(host != null);
                globalClient.createConnection(host, "program", "none");
            }
        }

        client = globalClient;
        connections++;
    }

    @Override
    public void syncUp() throws Exception {
        // this call blocks until all outstanding async calls have returned
        client.drain();
    }

    @Override
    public synchronized void close() throws Exception {
        // if this is the last connection open (all connections share one client),
        //  then close the connection
        if (--connections == 0) {
            globalClient = null;
            client.close();
        }
    }

    @Override
    public void queueBlobInsert(String key, byte[] value, RuntimeStats stats) throws Exception {
        // blob support for VoltDB is forthcoming... use string for now
        String safeValue = new String(value);

        AsyncCallback cb = new AsyncCallback(stats.puts);

        boolean queued = false;
        while (!queued) {
            // async invoke stored proc. cb callback gets called when response is received
            queued = client.callProcedure(cb, "InsertBlob", key, safeValue);

            // if queued if false, the server has told the client to slow down
            if (!queued) {
                // block until the VoltDB client thinks the server is ready for more work
                client.backpressureBarrier();
            }
        }
    }

    @Override
    public void queueInsertIntegerSet(String key, int[] values, RuntimeStats stats) throws Exception {
        AsyncCallback cb = new AsyncCallback(stats.puts);

        boolean queued = false;
        while (!queued) {
            // async invoke stored proc. cb callback gets called when response is received
            queued = client.callProcedure(cb, InsertInts.class.getSimpleName(), key, values);

            // if queued if false, the server has told the client to slow down
            if (!queued) {
                // block until the VoltDB client thinks the server is ready for more work
                client.backpressureBarrier();
            }
        }
    }

    @Override
    public void queueBlobGet(String key, RuntimeStats stats) throws Exception {
        AsyncCallback cb = new AsyncCallback(stats.gets);

        boolean queued = false;
        while (!queued) {
            // async invoke stored proc. cb callback gets called when response is received
            queued = client.callProcedure(cb, GetBlob.class.getSimpleName(), key);

            // if queued if false, the server has told the client to slow down
            if (!queued) {
                // block until the VoltDB client thinks the server is ready for more work
                client.backpressureBarrier();
            }
        }
    }

    @Override
    public void queueBlobPut(String key, byte[] value, RuntimeStats stats) throws Exception {
        AsyncCallback cb = new AsyncCallback(stats.puts);

        boolean queued = false;
        while (!queued) {
            // async invoke stored proc. cb callback gets called when response is received
            queued = client.callProcedure(cb, UpdateBlob.class.getSimpleName(), key, value);

            // if queued if false, the server has told the client to slow down
            if (!queued) {
                // block until the VoltDB client thinks the server is ready for more work
                client.backpressureBarrier();
            }
        }
    }

    @Override
    public void queueManyIntsOp(String key, int readIndex, int writeIndex, int randValue, RuntimeStats stats) throws Exception {
        AsyncCallback cb = new AsyncCallback(stats.puts);

        boolean queued = false;
        while (!queued) {
            // async invoke stored proc. cb callback gets called when response is received
            queued = client.callProcedure(cb, ManyInts.class.getSimpleName(), key, readIndex, writeIndex, randValue);

            // if queued if false, the server has told the client to slow down
            if (!queued) {
                // block until the VoltDB client thinks the server is ready for more work
                client.backpressureBarrier();
            }
        }
    }

    @Override
    public void queueManyIntsOpBatched(String key, int[] readIndices, int[] writeIndices, int[] randValues, RuntimeStats stats) throws Exception {
        AsyncCallback cb = new AsyncCallback(stats.puts);

        boolean queued = false;
        while (!queued) {
            // async invoke stored proc. cb callback gets called when response is received
            queued = client.callProcedure(cb, ManyIntsBatched.class.getSimpleName(), key, readIndices, writeIndices, randValues);

            // if queued if false, the server has told the client to slow down
            if (!queued) {
                // block until the VoltDB client thinks the server is ready for more work
                client.backpressureBarrier();
            }
        }
    }

    /**
     * Build a VoltDB application catalog for the benchmark with a given
     * deployment configuration. Note: K=0 means 1 copy, K=1 means 2 copies, etc..
     * Leader is only special during cluster bootstrap; it can be any node.
     *
     * @param args Array of:
     *   Site Per Host, Number of Hosts, K-Factor, Hostname of Leader
     */
    public static void main(String[] args) {
        // defaults
        int sitesPerHost = 1;
        int hosts = 1;
        int kfactor = 0;
        String leader = "localhost";

        // parse args
        if (args.length > 0)
            sitesPerHost = Integer.parseInt(args[0]);
        if (args.length > 1)
            hosts = Integer.parseInt(args[1]);
        if (args.length > 2)
            kfactor = Integer.parseInt(args[2]);
        if (args.length > 3)
            leader = args[3].trim();

        // procs used by voltdb
        Class<?>[] procedures = {
                GetBlob.class, UpdateBlob.class, InsertInts.class, ManyInts.class, ManyIntsBatched.class
        };

        // VoltProjectBuilder is a programmatic way to build VoltDB Project XML files.
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(VoltConnection.class.getResource("ddl.sql"));
        builder.addProcedures(procedures);
        builder.addStmtProcedure("InsertBlob", "INSERT INTO BLOBDATA VALUES (?, ?);");
        builder.addPartitionInfo("BLOBDATA", "KEYCOLUMN");
        builder.addPartitionInfo("MANYINTS", "KEYCOLUMN");
        builder.compile("kv.jar", sitesPerHost, hosts, kfactor, leader);
    }


}
