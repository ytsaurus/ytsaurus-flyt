package org.example;

import org.apache.flink.table.functions.ScalarFunction;

public class SimpleHashUdf extends ScalarFunction {

    public int eval(String s) {
        if (s == null) {
            return 0;
        }
        return Math.floorMod(s.hashCode(), 1000);
    }
}
