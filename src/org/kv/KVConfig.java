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
import java.io.FileReader;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * KVConfig holds the parameters to a benchmark. It can be set manually or loaded
 * from a JSON file. It can also serialize to JSON.
 *
 */
public class KVConfig {

	///////////////////////////////
    // CONSTANTS (for now)
	///////////////////////////////

	// how many columns in benchmarks 2+3
    public static final int INT_COLUMN_COUNT = 50;
    public static final int KEY_SIZE = 50;

    public static final int BLOB_BENCH = 1;
    public static final int INTS_BENCH = 2;
    public static final int BATCH_BENCH = 3;

	///////////////////////////////
    // AUTO-DISCOVERED PARAMS
	///////////////////////////////

    // useful for logging which machine was the client
    public final String hostname;

    ///////////////////////////////
	// GENERAL PARAMETERS
    ///////////////////////////////

    // Which storagelayer to use
	public String storageType = "VOLTDB";
	// Which operation to measure
	public int benchmarkLevel = 1;
	// How many threads/connections sending ops
	public int connectionCount = 1;
	// How long to measure
	public int testDurationSecs = 30;
	// How big is the value in the key-value op
	public int valueSizeInBytes = 1000;
	// How many keys to preload into the benchmark
	public int initialSize = 1000;
	// What is the fraction of gets and puts in benchmark 1
	public double getsFraction = 0.5;
	// Which hosts are running the storage (where clients connect)
	public String[] serverHosts = new String[] { "localhost" };
	// How many ops per key in operation 3
	public int bundleSize = 10;
	// Load the initial set (1) or assume it's already loaded (0)
	public int isLoader = 1;
	// Pause after load or startup until a keypress (1) or not (0)
	public int waitForKeypress = 0;

	// string value gets copied to log but is ignored by benchmark code
	@SuppressWarnings("unused")
	private String metaconfig = "";

	///////////////////////////////
	// VOLTDB SPECIFIC PARAMETERS
	///////////////////////////////

	///////////////////////////////
	// MYSQL SPECIFIC PARAMETERS
	///////////////////////////////

	public String mysqlStorageEngine = "INNODB";
	public String mysqlUser = "root";
	public String mysqlPassword = "mysql";
	// both of these next options are just experiments that don't seem
	// to make mysql much faster, but I could be doing it wrong...
	public int mysqlUseMemcache = 0; // 1 or 0
	public int mysqlUseProcedures = 0; // 1 or 0

	///////////////////////////////
	// CASSANDRA SPECIFIC PARAMETERS
	///////////////////////////////

	// see Cassandra docs for what these are
	public String cassWriteConsistencyLevel = "ANY";
	public String cassReadConsistencyLevel = "ONE";

	/**
	 * Constructor uses the default values but also sets the client's hostname.
	 */
	public KVConfig() {
		String tmpHostname = "unknown";
        try {
            tmpHostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.err.println("Unable to get hostname.");
        }
        // because hostname is final
        hostname = tmpHostname;
	}

	/**
	 * Code to read a json file into this structure.
	 * Sorta possible with the json library, but this works
	 *
	 * @param configFile File to read.
	 * @return A KVConfig instance with values set from the file.
	 */
    public static KVConfig load(File configFile) {
		try {
			BufferedReader in = new BufferedReader(new FileReader(configFile));
			KVConfig config = new KVConfig();

			// read the whole file into a single string
			String jsonStr = "";
			String line;
			while ((line = in.readLine()) != null)
				jsonStr += line + "\n";
			in.close();

			// build a json object from the string
			JSONObject json = new JSONObject(jsonStr);

			// while there are keys, set the members of the KVConfig instance
			Iterator<?> keyiter = json.keys();
			while (keyiter.hasNext()) {
			    String key = (String) keyiter.next();
			    Field field = KVConfig.class.getField(key);

			    // handle arrays first, then scalars
			    if (field.getType().isArray()) {
			        JSONArray array = json.getJSONArray(key);
			        Class<?> component = field.getType().getComponentType();

			        Object value = Array.newInstance(component, array.length());
			        for (int i = 0; i < array.length(); i++) {
			            if (component == int.class) {
			                Array.setInt(value, i, array.getInt(i));
			            }
		                if (component == long.class) {
		                    Array.setLong(value, i, array.getLong(i));
		                }
		                else if (component == String.class) {
		                    Array.set(value, i, array.getString(i));
		                }
		                else if (component == double.class) {
		                    Array.setDouble(value, i, array.getDouble(i));
		                }
			        }

			        field.set(config, value);
			    }
			    else if (field.getType() == int.class) {
			        field.setInt(config, json.getInt(key));
			    }
			    else if (field.getType() == long.class) {
			        field.setLong(config, json.getLong(key));
			    }
			    else if (field.getType() == String.class) {
			        field.set(config, json.getString(key));
			    }
			    else if (field.getType() == double.class) {
			        field.set(config, json.getDouble(key));
			    }
			}

			return config;

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return null;
	}

    /**
     * Serialized to json for logging using reflection.
     */
	@Override
	public String toString() {
	    JSONObject json = new JSONObject();

	    try {
    		Field[] fields = getClass().getFields();
    		for (Field field : fields) {
    		    if (field.getType().isArray()) {
    		        Object[] avals = (Object[]) field.get(this);
    		        for (Object aval : avals)
    		            json.append(field.getName(), aval);
    		    }
    		    else {
    		        json.put(field.getName(), field.get(this));
    		    }
    		}
	    }
	    catch (Exception e) {
	        e.printStackTrace();
	        System.exit(-1);
	    }

		return json.toString();
	}
}
