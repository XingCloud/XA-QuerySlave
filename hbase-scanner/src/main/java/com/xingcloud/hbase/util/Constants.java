package com.xingcloud.hbase.util;

import org.apache.hadoop.hbase.util.Pair;

import java.text.DecimalFormat;
import java.util.*;

/**
 * User: IvyTang
 * Date: 13-4-1
 * Time: 下午3:55
 */
public class Constants {

    public enum Operator {
        // Do not filter anything.
        ALL,
        // >
        GT,
        // <
        LT,
        // >=
        GE,
        // <=
        LE,
        // ==
        EQ,
        // !=
        NE,
        // between ... and ...[x, y]
        BETWEEN
    }

}
