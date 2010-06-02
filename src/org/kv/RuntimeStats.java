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

import java.util.concurrent.atomic.AtomicLong;

import org.json.JSONObject;

/**
 * Store the state of a current benchmark run. Storage layers can
 * call methods to start and stop operations.
 *
 */
public class RuntimeStats {

    /**
     * Statistics for a single operation, like a GET or PUT.
     */
    public class ProcStats {
        // affected by sending a request
        public final AtomicLong numSent = new AtomicLong(0);

        // affected by receiving a response
        public long numReceived = 0;
        // timing is only used if checking latency
        public long minExecutionNanos = Long.MAX_VALUE;
        public long maxExecutionNonos = Long.MIN_VALUE;
        public long totExecutionNanos = 0;

        /**
         * Record that an op was sent/queued to a storage layer.
         *
         * @return Time in ns when the proc was called or 0 if you don't care.
         */
        public long noteSent() {
            numSent.incrementAndGet();
            if (checkLatency) return System.nanoTime();
            else return 0;
        }

        /**
         * Record that an op has completed and returned to the client.
         *
         * @param startTime Time in ns when the proc was called or 0 if you don't care.
         */
        public synchronized void noteReceived(long startTime) {
            numReceived++;
            if (checkLatency) {
                long duration = startTime - System.nanoTime();
                if (duration < minExecutionNanos)
                    minExecutionNanos = duration;
                if (duration > maxExecutionNonos)
                    maxExecutionNonos = duration;
                totExecutionNanos += duration;
            }
        }
    }

    // do you want to record latency
    final boolean checkLatency;

    // stuff from KVConfig
    public final int valueSize;
    public final String hostname;

    // timing
    public long startTimeMS = 0;
    private long elapsedTimeMS = 0;

    // all benchmarks have one or two ops, use puts when there's only one
    public final ProcStats gets = new ProcStats();
    public final ProcStats puts = new ProcStats();

    /**
     * Initialize a benchmark result keeper object.
     *
     * @param config Benchmark configuration
     * @param checkLatency Should the benchmark measure latency.
     */
    public RuntimeStats(KVConfig config, boolean checkLatency) {
        valueSize = config.valueSizeInBytes;
        hostname = config.hostname;
        this.checkLatency = checkLatency;
    }

    /**
     * @return The number of total ops sent/queued.
     */
    public long numSent() {
        return puts.numSent.get() + gets.numSent.get();
    }

    /**
     * Record the beginning of a benchmark.
     */
    public void startExecution() {
        startTimeMS = System.currentTimeMillis();
    }

    /**
     * Record the end of a benchmark.
     */
    public void endExecution() {
        elapsedTimeMS = System.currentTimeMillis() - startTimeMS;
        // avoid divide by zero
        if (elapsedTimeMS == 0) { elapsedTimeMS = 1; };
    }

    /**
     * @return The elapsed time of the benchmark.
     */
    public long getElapsedTimeMS() {
        return System.currentTimeMillis() - startTimeMS;
    }

    /**
     * @return A JSON representation of the results for logging.
     */
    public String toJSONString() {
        double rate = (gets.numReceived + puts.numReceived) / (elapsedTimeMS / 1000.0);

        JSONObject json = new JSONObject();
        try {
            json.put("rate", rate);
            json.put("hostname", hostname);
            json.put("duration", elapsedTimeMS / 1000.0);
            json.put("gets", gets.numReceived);
            json.put("puts", puts.numReceived);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return json.toString();
    }

    /**
     * @return A human readable results string.
     */
    @Override
    public String toString() {
        return String.format("SUMMARY: %.2f ops/sec (%d ops in %.2f sec)",
                             (gets.numReceived + puts.numReceived) / (elapsedTimeMS / 1000.0),
                             gets.numReceived + puts.numReceived,
                             elapsedTimeMS / 1000.0);
    }
}
