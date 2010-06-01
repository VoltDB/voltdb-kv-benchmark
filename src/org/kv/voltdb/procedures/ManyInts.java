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

public class ManyInts extends VoltProcedure {
    // check if key exists
    public final SQLStmt get = new SQLStmt("SELECT * FROM MANYINTS WHERE KEYCOLUMN = ?;");

    // update key/value
    // not pretty, but it's a silly thing the proc is doing
    public final String uPrefix = "UPDATE MANYINTS SET COL";
    public final String uSuffix = " = ? WHERE KEYCOLUMN = ?;";
    public final SQLStmt u00 = new SQLStmt(uPrefix + "00" + uSuffix);
    public final SQLStmt u01 = new SQLStmt(uPrefix + "01" + uSuffix);
    public final SQLStmt u02 = new SQLStmt(uPrefix + "02" + uSuffix);
    public final SQLStmt u03 = new SQLStmt(uPrefix + "03" + uSuffix);
    public final SQLStmt u04 = new SQLStmt(uPrefix + "04" + uSuffix);
    public final SQLStmt u05 = new SQLStmt(uPrefix + "05" + uSuffix);
    public final SQLStmt u06 = new SQLStmt(uPrefix + "06" + uSuffix);
    public final SQLStmt u07 = new SQLStmt(uPrefix + "07" + uSuffix);
    public final SQLStmt u08 = new SQLStmt(uPrefix + "08" + uSuffix);
    public final SQLStmt u09 = new SQLStmt(uPrefix + "09" + uSuffix);
    public final SQLStmt u10 = new SQLStmt(uPrefix + "10" + uSuffix);
    public final SQLStmt u11 = new SQLStmt(uPrefix + "11" + uSuffix);
    public final SQLStmt u12 = new SQLStmt(uPrefix + "12" + uSuffix);
    public final SQLStmt u13 = new SQLStmt(uPrefix + "13" + uSuffix);
    public final SQLStmt u14 = new SQLStmt(uPrefix + "14" + uSuffix);
    public final SQLStmt u15 = new SQLStmt(uPrefix + "15" + uSuffix);
    public final SQLStmt u16 = new SQLStmt(uPrefix + "16" + uSuffix);
    public final SQLStmt u17 = new SQLStmt(uPrefix + "17" + uSuffix);
    public final SQLStmt u18 = new SQLStmt(uPrefix + "18" + uSuffix);
    public final SQLStmt u19 = new SQLStmt(uPrefix + "19" + uSuffix);
    public final SQLStmt u20 = new SQLStmt(uPrefix + "20" + uSuffix);
    public final SQLStmt u21 = new SQLStmt(uPrefix + "21" + uSuffix);
    public final SQLStmt u22 = new SQLStmt(uPrefix + "22" + uSuffix);
    public final SQLStmt u23 = new SQLStmt(uPrefix + "23" + uSuffix);
    public final SQLStmt u24 = new SQLStmt(uPrefix + "24" + uSuffix);
    public final SQLStmt u25 = new SQLStmt(uPrefix + "25" + uSuffix);
    public final SQLStmt u26 = new SQLStmt(uPrefix + "26" + uSuffix);
    public final SQLStmt u27 = new SQLStmt(uPrefix + "27" + uSuffix);
    public final SQLStmt u28 = new SQLStmt(uPrefix + "28" + uSuffix);
    public final SQLStmt u29 = new SQLStmt(uPrefix + "29" + uSuffix);
    public final SQLStmt u30 = new SQLStmt(uPrefix + "30" + uSuffix);
    public final SQLStmt u31 = new SQLStmt(uPrefix + "31" + uSuffix);
    public final SQLStmt u32 = new SQLStmt(uPrefix + "32" + uSuffix);
    public final SQLStmt u33 = new SQLStmt(uPrefix + "33" + uSuffix);
    public final SQLStmt u34 = new SQLStmt(uPrefix + "34" + uSuffix);
    public final SQLStmt u35 = new SQLStmt(uPrefix + "35" + uSuffix);
    public final SQLStmt u36 = new SQLStmt(uPrefix + "36" + uSuffix);
    public final SQLStmt u37 = new SQLStmt(uPrefix + "37" + uSuffix);
    public final SQLStmt u38 = new SQLStmt(uPrefix + "38" + uSuffix);
    public final SQLStmt u39 = new SQLStmt(uPrefix + "39" + uSuffix);
    public final SQLStmt u40 = new SQLStmt(uPrefix + "40" + uSuffix);
    public final SQLStmt u41 = new SQLStmt(uPrefix + "41" + uSuffix);
    public final SQLStmt u42 = new SQLStmt(uPrefix + "42" + uSuffix);
    public final SQLStmt u43 = new SQLStmt(uPrefix + "43" + uSuffix);
    public final SQLStmt u44 = new SQLStmt(uPrefix + "44" + uSuffix);
    public final SQLStmt u45 = new SQLStmt(uPrefix + "45" + uSuffix);
    public final SQLStmt u46 = new SQLStmt(uPrefix + "46" + uSuffix);
    public final SQLStmt u47 = new SQLStmt(uPrefix + "47" + uSuffix);
    public final SQLStmt u48 = new SQLStmt(uPrefix + "48" + uSuffix);
    public final SQLStmt u49 = new SQLStmt(uPrefix + "49" + uSuffix);

    // access the sql statements as an indexed array
    // again... hacking around the weirdness
    public final SQLStmt[] updates = {
            u00, u01, u02, u03, u04, u05, u06, u07, u08, u09,
            u10, u11, u12, u13, u14, u15, u16, u17, u18, u19,
            u20, u21, u22, u23, u24, u25, u26, u27, u28, u29,
            u30, u31, u32, u33, u34, u35, u36, u37, u38, u39,
            u40, u41, u42, u43, u44, u45, u46, u47, u48, u49,
    };

    public long run(String strKey, int readIndex, int writeIndex, int randValue) {

        // read a particular column from a specific row
        voltQueueSQL(get, strKey);
        VoltTable[] results = voltExecuteSQL();
        VoltTable decisionTable = results[0];
        if (decisionTable.getRowCount() == 0)
            throw new VoltAbortException("Get in manyints missed for key: " + strKey);
        decisionTable.resetRowPosition();
        decisionTable.advanceRow();
        // add one to the index for the key column
        long decisionValue = decisionTable.getLong(readIndex + 1);

        // if the chosen column is odd, update another column
        if ((decisionValue % 2) == 1) {
            voltQueueSQL(updates[writeIndex], randValue, strKey);
            results = voltExecuteSQL();
            if (results[0].asScalarLong() != 1)
                throw new VoltAbortException("Failed manyints column update for key: " + strKey);
            return 1;
        }

        return 0;
    }
}