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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.kv.cassandra.CassandraConnection;
import org.kv.myql.MySQLConnection;
import org.kv.voltdb.VoltConnection;

/**
 * Manages the loading and running of a benchmark with a specific
 * configuration (KVConfig) and StorageLayer.
 *
 */
public class Driver {

	// where to log, probably should be in the KVConfig
	static final String OUTPUT_FILENAME = "kvbench-log.txt";
	static File logfile = new File(OUTPUT_FILENAME);

	// benchmakr configuration
    KVConfig config;

    // payload
    byte[] baseValue;

    // set of loaded keys
    String[] keys;

    /**
     * Load a configuration. Generate a payload.
     *
     * @param config Benchmark configuration to use.
     */
	public Driver(KVConfig config) {
		assert(config != null);

		System.out.println("JSONCONFIG " + config.toString());
		// start each benchmark with an empty line
		// this allows easy reading of the log with multiple benchmarks
		log("");
		// log the benchmark configuration
		log(config.toString());

		this.config = config;

		// seed this deterministically to always generate the same keyset
		//  especially useful if running two clients on two machines
		Random rand = new Random(0);

		// Use the same value for all keys for benchmark 1.
		//  This seems lame, but I'm not sure it matters. Would love to fix
		//  it. For now, it's dead simple. I know VoltDB doesn't do anything
		//  smart.
		baseValue = new byte[config.valueSizeInBytes];
		// create random/not easily compressible payload
		for (int i = 0; i < baseValue.length; i++) {
		    // pick a random value between 1 and 127 inclusive
			// excludes zeros to make it a real (sorta) string
		    baseValue[i] = (byte) (rand.nextInt(127) + 1);
		}

		// If using mysql, reset the schema and tables
		if (config.storageType.compareToIgnoreCase("MYSQL") == 0) {
			MySQLConnection.resetData(config);
		}

		// generate all the keys
		// keys are a random mix of syllables pulled from the TPC-C data generator
		// along with a number at the end representing the index of the key

		final String SYLLABLES[] = {
                "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
        };
		keys = new String[config.initialSize];
		// save 10 bytes at the end for the string rep of the key index
		final int randLength = KVConfig.KEY_SIZE - 10;
		for (int i = 0; i < config.initialSize; i++) {
		    String keypart = String.format("%10s", i);
		    String randpart = "";
		    while (randpart.length() < (KVConfig.KEY_SIZE - 10)) {
		        randpart += SYLLABLES[rand.nextInt(SYLLABLES.length)];
		    }
		    randpart = randpart.substring(0, randLength);
		    keys[i] = randpart + keypart;
		    assert(keys[i].length() == KVConfig.KEY_SIZE);
		}
	}

	/**
	 * Append a string to the log.
	 * @param str Value to append.
	 */
	static void log(String str) {
        try {
            FileWriter log = new FileWriter(logfile, true);
            log.write(str + "\n");
            log.flush();
            log.close();
        } catch (IOException e) {
        	// fail fast
            e.printStackTrace();
            System.exit(-1);
        }
    }

	/**
	 * Using the values from the KVConfig, instantiate and connect an
	 * appropriate StorageLayerConnection for the requested implementation.
	 *
	 * @return An initialized and connected StorageLayerConnection for a
	 * specific implementation.
	 */
	StorageLayerConnection connectionFactory() {
		StorageLayerConnection storage = null;

		if (config.storageType.compareToIgnoreCase("VOLTDB") == 0) {
			storage = new VoltConnection();
		}
		else if (config.storageType.compareToIgnoreCase("MYSQL") == 0) {
			storage = new MySQLConnection();
		}
		else if (config.storageType.compareToIgnoreCase("CASSANDRA") == 0) {
			storage = new CassandraConnection();
		}

		try {
			storage.initializeAndConnect(config);
		}
		catch (Exception e) {
			// fail fast
            e.printStackTrace();
            System.exit(-1);
        }

		return storage;
	}

	/**
	 * Load the requested numbers of keys into the storage.
	 * This is single-threaded and single-connection, which
	 * makes some backends slow to load. Could be improved.
	 */
	public void initializeData() {
		if (config.initialSize == 0) {
			return;
		}

		try {
			// seed with unique stuff to ensure not the same order as the bench
			long seed = System.currentTimeMillis() / 2;
			final Random rand = new Random(seed);

			// create the object to track the results
			RuntimeStats stats = new RuntimeStats(config, false);
			// get a connection to the storage layer
			StorageLayerConnection storage = connectionFactory();

			System.out.println("BEGIN LOAD");

			// note the load has started
	        stats.startExecution();

	        // load the right number of keys
            int loaded;
            for (loaded = 0; loaded < config.initialSize; ++loaded) {
            	String key = keys[loaded];
            	if (config.benchmarkLevel == KVConfig.BLOB_BENCH) {
                    storage.queueBlobInsert(key, baseValue, stats);
            	}
            	else {
            	    int[] randValues = new int[KVConfig.INT_COLUMN_COUNT];
            	    for (int i = 0; i < KVConfig.INT_COLUMN_COUNT; i++) {
            	        randValues[i] = rand.nextInt();
            	    }
            	    storage.queueInsertIntegerSet(key, randValues, stats);
            	}
            }
            assert(loaded == stats.puts.numSent.get());

            // needed for VoltDB. Wait for all the async calls to complete.
            storage.syncUp();

            // note the load has finished
            stats.endExecution();

            storage.close();

            System.out.print("LOADING ");
            System.out.println(stats);
        }
        catch (Exception e) {
        	// fail fast
            e.printStackTrace();
            System.exit(-1);
        }
   	}

	/**
	 * Thread that actually runs as many ops as possible using a single
	 * client connection to storage. Used by the runBenchmark() method.
	 *
	 */
	class Worker extends Thread {

		// connect to the storage
		final StorageLayerConnection storage = connectionFactory();

		// seed with something different for each thread
		final Random rand = new Random(Thread.currentThread().getId() + System.currentTimeMillis() % 10000);

		// results object shared among all clients
		final RuntimeStats stats;

		// used to know when to stop
		public final AtomicBoolean shouldContinue = new AtomicBoolean(true);

		// get the stats object for this benchmark
		public Worker(RuntimeStats stats) {
			this.stats = stats;
		}

		@Override
		public void run() {
		    try {
		    	// continue until the driver says stop
    			while (shouldContinue.get()) {

    				// pick a key
    				int currentKey = rand.nextInt(config.initialSize);
    				String key = keys[currentKey];

    				// BENCHMARK 1
    				if (config.benchmarkLevel == KVConfig.BLOB_BENCH) {
        	            if (rand.nextDouble() < config.getsFraction) {
        	                storage.queueBlobGet(key, stats);
        	            }
        	            else {
        	                storage.queueBlobPut(key, baseValue, stats);
        	            }
    				}

    				// BENCHMARK 2
    				else if (config.benchmarkLevel == KVConfig.INTS_BENCH) {
    				    int readIndex = rand.nextInt(KVConfig.INT_COLUMN_COUNT);
    				    int writeIndex = rand.nextInt(KVConfig.INT_COLUMN_COUNT);
    				    int randValue = rand.nextInt();

    				    storage.queueManyIntsOp(key, readIndex, writeIndex, randValue, stats);
    				}

    				// BENCHMARK 3
    				else if (config.benchmarkLevel == KVConfig.BATCH_BENCH) {
    				    int[] readIndices = new int[config.bundleSize];
                        int[] writeIndices = new int[config.bundleSize];
                        int[] randValues = new int[config.bundleSize];
                        for (int i = 0; i < config.bundleSize; i++) {
                            readIndices[i] = rand.nextInt(KVConfig.INT_COLUMN_COUNT);
                            writeIndices[i] = rand.nextInt(KVConfig.INT_COLUMN_COUNT);
                            randValues[i] = rand.nextInt();
                        }

                        storage.queueManyIntsOpBatched(key, readIndices, writeIndices, randValues, stats);
    				}
    			}

    			// needed for VoltDB. Wait for all the async calls to complete.
    			storage.syncUp();

    			storage.close();
		    }
		    catch (Exception e) {
		    	// fail fast
		        e.printStackTrace();
		        System.exit(-1);
		    }
		}

	}

	/**
	 * Run the benchmark for the loaded configuration.
	 *  (Includes loading if requested.)
	 */
	public void runBenchmark() {
		try {
			// load keys
		    if (config.isLoader == 1)
		        initializeData();

		    // if needed, wait for keypress (useful for multiple client machines)
		    if (config.waitForKeypress == 1) {
		        System.out.println("PRESS ENTER OR RETURN TO CONTINUE");
		        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
		        in.readLine();
		    }

		    // Create the object to record the benchmark results and progress
			RuntimeStats stats = new RuntimeStats(config, false);

			System.out.println("BEGIN BENCHMARK");

			// create the requested number of client threads and connections
			ArrayList<Worker> workerThreads = new ArrayList<Worker>();
	        for (int i = 0; i < config.connectionCount; i++) {
	            final Worker worker = new Worker(stats);
	            workerThreads.add(worker);
	        }

	        // start the timer on the benchmark
	        stats.startExecution();

	        // start the benchmark
	        for (Worker worker : workerThreads)
	        	worker.start();

	        // sleep while the benchmark is running
	        while (stats.getElapsedTimeMS() < (config.testDurationSecs * 1000)) {
	        	Thread.sleep(1000);
	        }

	        // tell all the workers to stop
	        for (Worker worker : workerThreads)
	        	worker.shouldContinue.set(false);

	        // wait for all the workers to have stopped
	        for (Worker worker : workerThreads) {
	        	worker.join();
	        }

	        // stop the benchmark... note, the timing (and thus the throughput measurement) includes
	        // collecting any async calls and joining the client threads. Trying to be fair.
	        stats.endExecution();

	        // print out results to console
	        System.out.println("JSONRESULTS " + stats.toJSONString());
	        System.out.print("BENCHMARK ");
	        System.out.println(stats);

	        // log the benchmark results
	        log(stats.toJSONString());
		}
        catch (Exception e) {
        	// fail fast
			e.printStackTrace();
			System.exit(-1);
        }
	}

	/**
	 * @param args An array containing one value with the name of the file with
	 * configuration.
	 */
	public static void main(String[] args) {
		for (String arg : args)
			System.out.println(arg);

		if (args.length != 1) {
			System.err.println("Expects 1 arg containing config file name");
			System.exit(-1);
		}

		// make sure the file exists and load config data from it
		File configFile = new File(args[0]);
		KVConfig config = KVConfig.load(configFile);
		if (config == null) {
			System.err.println("Failed to parse config file");
			System.exit(-1);
		}

		// do the benchmark
        Driver driver = new Driver(config);
        driver.runBenchmark();
	}
}
