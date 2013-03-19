package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-7
 * Time: 下午5:39
 * To change this template use File | Settings | File Templates.
 */
public interface XAScanner {
    public boolean next(List<KeyValue> results) throws IOException;
    public void close() throws IOException;

}
