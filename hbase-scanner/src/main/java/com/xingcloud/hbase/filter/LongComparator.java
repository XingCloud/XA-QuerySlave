package com.xingcloud.hbase.filter;


import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

public class LongComparator extends WritableByteArrayComparable {
    public LongComparator() {
        super();
    }

    public LongComparator(byte [] value) {
        super(value);
    }

    @Override
    public int compareTo(byte[] value) {
        long dis = Bytes.toLong(this.getValue()) - Bytes.toLong(value);
        if (dis > 0) {
            return 1;
        } else if (dis < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public int compareTo(byte[] arg0, int offset, int length) {
        byte[] val = Arrays.copyOfRange(arg0, offset, offset + length);
        return compareTo(val);
    }
}
