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

package org.kv.voltdb.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        partitionInfo = "MANYINTS.KEYCOLUMN: 0",
        singlePartition = true
)
public class InsertInts extends VoltProcedure {

    public final SQLStmt insert = new SQLStmt("INSERT INTO MANYINTS VALUES (" +
    		"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
    		"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
    		"?, ?, ?, ?, ?, ?, ?);");

    public VoltTable[] run(String strKey, int[] i) {

    	// this isn't pretty, but rarely do you need to do something this dumb
    	voltQueueSQL(insert, strKey,
    			i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9],
    			i[10], i[11], i[12], i[13], i[14], i[15], i[16], i[17], i[18], i[19],
    			i[20], i[21], i[22], i[23], i[24], i[25], i[26], i[27], i[28], i[29],
    			i[30], i[31], i[32], i[33], i[34], i[35], i[36], i[37], i[38], i[39],
    			i[40], i[41], i[42], i[43], i[44], i[45], i[46], i[47], i[48], i[49]);
    	return voltExecuteSQL();
    }
}
