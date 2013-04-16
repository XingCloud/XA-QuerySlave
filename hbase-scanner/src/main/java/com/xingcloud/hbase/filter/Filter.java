package com.xingcloud.hbase.filter;

import com.xingcloud.hbase.util.Constants;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Filter implements Writable {

    public static final Filter ALL = new Filter(Constants.Operator.ALL, 0, 0);

    private Constants.Operator operator;

    private long value;

    private long extraValue;

    public Filter() {
        super();
    }

    public Filter(Constants.Operator operator) {
        super();
        this.operator = operator;
    }

    public Filter(Constants.Operator operator, long value, long extraValue) {
        super();
        this.operator = operator;
        this.value = value;
        this.extraValue = extraValue;
    }

    public Filter(Constants.Operator operator, long value) {
        super();
        this.operator = operator;
        this.value = value;
    }

    public Constants.Operator getOperator() {
        return operator;
    }

    public void setOperator(Constants.Operator operator) {
        this.operator = operator;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getExtraValue() {
        return extraValue;
    }

    public void setExtraValue(long extraValue) {
        this.extraValue = extraValue;
    }

    public String toString() {
        return "VF-" + operator + "-" + value + "-" + extraValue;
    }

    public static Filter parseToFilter(String filterStr) {
        String[] fields = filterStr.split("-");
        Constants.Operator o = getOperator(fields[1]);
        long val = Long.parseLong(fields[2]);
        long extraVal = Long.parseLong(fields[3]);

        Filter filter = new Filter(o, val, extraVal);
        return filter;
    }

    public static Constants.Operator getOperator(String name) {
        for (Constants.Operator o : Constants.Operator.values()) {
            if (name.equals(o.name())) {
                return o;
            }
        }
        throw new IllegalArgumentException("Can't parse filter operator of " + name);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        Constants.Operator operator = WritableUtils.readEnum(in, Constants.Operator.class);
        this.operator = operator;
        this.value = WritableUtils.readVLong(in);
        this.extraValue = WritableUtils.readVLong(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeEnum(out, operator);
        WritableUtils.writeVLong(out, this.value);
        WritableUtils.writeVLong(out, this.extraValue);
    }

    public static void printBytes(byte[] bytes) {
        for (byte b : bytes) {
            System.out.print(b);
        }
        System.out.println();
    }

    public boolean checkVal(long val) {
        Constants.Operator operator = getOperator();
        if (getOperator() == Constants.Operator.ALL) {
            return true;
        }
        long filterVal = getValue();
        switch (operator) {
            case LT:
                return val < filterVal;
            case LE:
                return val <= filterVal;
            case GT:
                return val > filterVal;
            case GE:
                return val >= filterVal;
            case EQ:
                return val == filterVal;
            case NE:
                return val != filterVal;
            case BETWEEN:
                long filterExtraVal = getExtraValue();
                return val >= filterVal && val <= filterExtraVal;
        }
        return false;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (extraValue ^ (extraValue >>> 32));
        result = prime * result
                + ((operator == null) ? 0 : operator.hashCode());
        result = prime * result + (int) (value ^ (value >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Filter other = (Filter) obj;
        if (extraValue != other.extraValue)
            return false;
        if (operator != other.operator)
            return false;
        if (value != other.value)
            return false;
        return true;
    }

}
