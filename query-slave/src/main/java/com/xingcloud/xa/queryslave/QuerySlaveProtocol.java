package com.xingcloud.xa.queryslave;

import net.sf.jsqlparser.JSQLParserException;
import org.apache.hadoop.io.MapWritable;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 3/15/13
 * Time: 3:56 AM
 * To change this template use File | Settings | File Templates.
 */
public interface QuerySlaveProtocol {
    public MapWritable query(String sql) throws IOException, JSQLParserException;
}
