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

package org.kv.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.kv.KVConfig;
import org.kv.RuntimeStats;
import org.kv.StorageLayerConnection;

public class CassandraConnection extends StorageLayerConnection {

    static final String KEYSPACE = "Keyspace1";
    static final String BLOB_COL_FAMILY = "BlobData";
    static final String INTS_COL_FAMILY = "ManyInts";

    // thrift transports to each server
    TTransport[] transports;
    // thrift clients for each server
    Cassandra.Client[] clients;
    // index of next server to use
    int currentClientIndex = 0;

    // see Cassandra docs for the meaning of these
    ConsistencyLevel writeConsistency;
    ConsistencyLevel readConsistency;

    @Override
    public void initializeAndConnect(KVConfig config) throws Exception {
        assert(config.serverHosts.length > 0);

        writeConsistency = consistencyLevelFromString(config.cassWriteConsistencyLevel);
        readConsistency = consistencyLevelFromString(config.cassReadConsistencyLevel);

        int hostCount = config.serverHosts.length;
        transports = new TTransport[hostCount];
        clients = new Cassandra.Client[hostCount];

        // for each server, connect
        for (int i = 0; i < hostCount; i++) {

            TTransport transport;
            transport = new TSocket(config.serverHosts[i], 9160);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            Cassandra.Client client = new Cassandra.Client(protocol);

            transports[i] = transport;
            clients[i] = client;
        }
    }

    @Override
    public void close() throws Exception {
        for (TTransport transport : transports) {
            transport.flush();
            transport.close();
        }
    }

    @Override
    public void queueBlobInsert(String key, byte[] value, RuntimeStats stats) throws Exception {
        long startTime = stats.puts.noteSent();

        long timestamp = System.currentTimeMillis();

        // The Cassandra layer supports overwriting of existing values
        // 1. Not sure this is the easiest way to do this
        // 2. It's just the loader so it's not part of the benchmark
        // 3. For a real benchmark, you should probably clear the data first

        if (keyExists(key, BLOB_COL_FAMILY)) {

            // if it exists, overwrite (following Cassandra examples)

            Column column = new Column("value".getBytes("utf-8"), value, timestamp);

            ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
            columnOrSuperColumn.setColumn(column);

            Mutation mutation = new Mutation();
            mutation.setColumn_or_supercolumn(columnOrSuperColumn);

            List<Mutation> mutations = new ArrayList<Mutation>();
            mutations.add(mutation);

            Map<String, List<Mutation>> job = new HashMap<String, List<Mutation>>();
            job.put(BLOB_COL_FAMILY, mutations);

            Map<String, Map<String, List<Mutation>>> batch =
                new HashMap<String, Map<String, List<Mutation>>>();
            batch.put(key, job);

            clients[currentClientIndex].batch_mutate(KEYSPACE, batch, writeConsistency);
        }
        else {

            // if no key, insert

            ColumnPath path = new ColumnPath(BLOB_COL_FAMILY);
            path.column = "value".getBytes("utf-8");

            clients[currentClientIndex].insert(KEYSPACE, key, path, value, timestamp, writeConsistency);
        }

        stats.puts.noteReceived(startTime);

        // round robin to the next server node
        currentClientIndex = (currentClientIndex + 1) % clients.length;
    }

    @Override
    public void queueInsertIntegerSet(String key, int[] values, RuntimeStats stats) throws Exception {
        long startTime = stats.puts.noteSent();

        // need a timestamp for Cassandra writes
        long timestamp = System.currentTimeMillis();

        List<Mutation> mutations = new ArrayList<Mutation>();

        // add the 50 column values
        for (byte i = 0; i < KVConfig.INT_COLUMN_COUNT; i++) {
            Column column = new Column(new byte[] {i}, intToByteArray(values[i]), timestamp);
            ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
            columnOrSuperColumn.setColumn(column);
            Mutation mutation = new Mutation();
            mutation.setColumn_or_supercolumn(columnOrSuperColumn);
            mutations.add(mutation);
        }

        assert(mutations.size() == KVConfig.INT_COLUMN_COUNT);

        Map<String, List<Mutation>> job = new HashMap<String, List<Mutation>>();
        job.put(INTS_COL_FAMILY, mutations);

        Map<String, Map<String, List<Mutation>>> batch = new HashMap<String, Map<String, List<Mutation>>>();
        batch.put(key, job);

        clients[currentClientIndex].batch_mutate(KEYSPACE, batch, writeConsistency);

        stats.puts.noteReceived(startTime);

        // round robin to the next server node
        currentClientIndex = (currentClientIndex + 1) % clients.length;
    }

    @Override
    public void queueBlobGet(String key, RuntimeStats stats) throws Exception {
        long startTime = stats.gets.noteSent();

        // we don't verify the data... probably should
        boolean exists = keyExists(key, BLOB_COL_FAMILY);
        assert(exists);

        stats.gets.noteReceived(startTime);

        // round robin to the next server node
        currentClientIndex = (currentClientIndex + 1) % clients.length;
    }

    @Override
    public void queueBlobPut(String key, byte[] value, RuntimeStats stats) throws Exception {
        long startTime = stats.puts.noteSent();

        // need a timestamp for Cassandra writes
        long timestamp = System.currentTimeMillis();

        Column column = new Column("value".getBytes("utf-8"), value, timestamp);

        ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
        columnOrSuperColumn.setColumn(column);

        Mutation mutation = new Mutation();
        mutation.setColumn_or_supercolumn(columnOrSuperColumn);

        List<Mutation> mutations = new ArrayList<Mutation>();
        mutations.add(mutation);

        Map<String, List<Mutation>> job = new HashMap<String, List<Mutation>>();
        job.put(BLOB_COL_FAMILY, mutations);

        Map<String, Map<String, List<Mutation>>> batch =
            new HashMap<String, Map<String, List<Mutation>>>();
        batch.put(key, job);

        clients[currentClientIndex].batch_mutate(KEYSPACE, batch, writeConsistency);

        stats.puts.noteReceived(startTime);

        // round robin to the next server node
        currentClientIndex = (currentClientIndex + 1) % clients.length;
    }

    @Override
    public void queueManyIntsOp(String key, int readIndex, int writeIndex, int randValue, RuntimeStats stats) throws Exception {
        long startTime = stats.puts.noteSent();

        ColumnPath path = new ColumnPath();
        path.setColumn_family(INTS_COL_FAMILY);
        path.setColumn(new byte[] { (byte) readIndex });

        ColumnOrSuperColumn result = clients[currentClientIndex].get(KEYSPACE, key, path, readConsistency);
        Column col = result.getColumn();
        int decisionValue = byteArrayToInt(col.getValue());

        // if the chosen column is odd, update another column
        if ((decisionValue % 2) == 1) {
            long timestamp = System.currentTimeMillis();
            byte[] randValueBytes = intToByteArray(randValue);

            path.setColumn(new byte[] { (byte) writeIndex });
            clients[currentClientIndex].insert(KEYSPACE, key, path, randValueBytes, timestamp, writeConsistency);
        }

        stats.puts.noteReceived(startTime);

        // round robin to the next server node
        currentClientIndex = (currentClientIndex + 1) % clients.length;
    }

    @Override
    public void queueManyIntsOpBatched(String key, int[] readIndices, int[] writeIndices, int[] randValues, RuntimeStats stats) throws Exception {
        long startTime = stats.puts.noteSent();

        // FETCH THE VALUE FOR THE KEY

        Map<String, List<ColumnOrSuperColumn>> results;

        SlicePredicate predicate = new SlicePredicate();

        for (int i = 0; i < readIndices.length; i++) {
            predicate.addToColumn_names(new byte[] { (byte) readIndices[i] });
        }

        ColumnParent parent = new ColumnParent();
        parent.setColumn_family(INTS_COL_FAMILY);

        ArrayList<String> keys = new ArrayList<String>();
        keys.add(key);

        results = clients[currentClientIndex].multiget_slice(KEYSPACE, keys, parent, predicate, readConsistency);

        assert(results.size() == 1);
        List<ColumnOrSuperColumn> columns = results.get(key);
        assert(columns != null);
        assert(columns.size() <= readIndices.length);
        assert(columns.size() > 0);

        // UPDATE WHATEVER COLUMNS NEED TO BE UPDATED

        long timestamp = System.currentTimeMillis();

        List<Mutation> mutations = new ArrayList<Mutation>();

        HashMap<Integer, Column> columnsByIndex = new HashMap<Integer, Column>();
        for (ColumnOrSuperColumn cosc : columns) {
            Column column = cosc.getColumn();
            int rindex = column.name[0];
            assert(rindex < KVConfig.INT_COLUMN_COUNT);
            columnsByIndex.put(rindex, column);
        }

        for (int i = 0; i < readIndices.length; i++) {
            Column readColumn = columnsByIndex.get(readIndices[i]);
            int decisionValue = byteArrayToInt(readColumn.getValue());

            // if the chosen column is odd, update another column
            if ((decisionValue % 2) == 1) {
                Column column = new Column(new byte[] { (byte) writeIndices[i] }, intToByteArray(randValues[i]), timestamp);
                ColumnOrSuperColumn columnOrSuperColumn = new ColumnOrSuperColumn();
                columnOrSuperColumn.setColumn(column);
                Mutation mutation = new Mutation();
                mutation.setColumn_or_supercolumn(columnOrSuperColumn);
                mutations.add(mutation);
            }
        }

        if (mutations.size() > 0) {
            Map<String, List<Mutation>> job = new HashMap<String, List<Mutation>>();
            job.put(INTS_COL_FAMILY, mutations);

            Map<String, Map<String, List<Mutation>>> batch = new HashMap<String, Map<String, List<Mutation>>>();
            batch.put(key, job);

            clients[currentClientIndex].batch_mutate(KEYSPACE, batch, writeConsistency);
        }

        stats.puts.noteReceived(startTime);

        // round robin to the next server node
        currentClientIndex = (currentClientIndex + 1) % clients.length;
    }

    /**
     * Translate and int to 4 bytes
     */
    private static byte[] intToByteArray(int i) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (i & 0x000000FF);
        bytes[1] = (byte) ((i >>> 8) & 0x000000FF);
        bytes[2] = (byte) ((i >>> 16) & 0x000000FF);
        bytes[3] = (byte) ((i >>> 24) & 0x000000FF);
        return bytes;
    }

    /**
     * Translate 4 bytes to an int
     */
    private static int byteArrayToInt(byte[] bytes) {
        int i = bytes[0];
        i += bytes[1] << 8;
        i += bytes[2] << 16;
        i += bytes[3] << 24;
        return i;
    }

    /**
     * Effectively do a get, but we don't care about the result.
     */
    private boolean keyExists(String key, String columnFamily) throws Exception {
        SlicePredicate slicePredicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[] {});
        sliceRange.setFinish(new byte[] {});
        slicePredicate.setSlice_range(sliceRange);

        List<ColumnOrSuperColumn> results =
            clients[currentClientIndex].get_slice(KEYSPACE, key, new ColumnParent(columnFamily), slicePredicate, readConsistency);

        return results.size() == 1;
    }

    /**
     * Dumb method to convert the config param for consistency to the right enum.
     */
    private ConsistencyLevel consistencyLevelFromString(String cLevel) {
        if (cLevel.compareToIgnoreCase("ZERO") == 0)
            return ConsistencyLevel.ZERO;
        if (cLevel.compareToIgnoreCase("ANY") == 0)
            return ConsistencyLevel.ANY;
        if (cLevel.compareToIgnoreCase("ONE") == 0)
            return ConsistencyLevel.ONE;
        if (cLevel.compareToIgnoreCase("QUORUM") == 0)
            return ConsistencyLevel.QUORUM;
        if (cLevel.compareToIgnoreCase("ALL") == 0)
            return ConsistencyLevel.ALL;
        System.err.printf("Consistency level \"%s\" is not valid.");
        System.exit(-1);
        return null;
    }
}
