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

/**
 * Abstraction of something that can do the arbitrary benchmark
 * operations we've defined.
 *
 */
public abstract class StorageLayerConnection {

    // methods for connection management
	public abstract void initializeAndConnect(KVConfig config) throws Exception;
	public void syncUp() throws Exception {} // needed for async voltdb more than others
    public abstract void close() throws Exception;

	// methods for data loading
	public abstract void queueBlobInsert(String key, byte[] value, RuntimeStats stats) throws Exception;
	public abstract void queueInsertIntegerSet(String key, int[] integers, RuntimeStats stats) throws Exception;

	// methods for blob operations
	public abstract void queueBlobPut(String key, byte[] value, RuntimeStats stats) throws Exception;
	public abstract void queueBlobGet(String key, RuntimeStats stats) throws Exception;

	// methods for multi-column integer operations
	public abstract void queueManyIntsOp(String key, int readIndex, int writeIndex, int randValue, RuntimeStats stats) throws Exception;

	// methods for batches of multi-column integer operations
	public abstract void queueManyIntsOpBatched(String key, int[] readIndices, int[] writeIndices, int[] randValues, RuntimeStats stats) throws Exception;
}
