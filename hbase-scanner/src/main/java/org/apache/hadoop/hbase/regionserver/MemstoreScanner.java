package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-7
 * Time: 下午6:46
 * To change this template use File | Settings | File Templates.
 */
public class MemstoreScanner implements DataScanner {
    private static Logger LOG = LoggerFactory.getLogger(MemstoreScanner.class);

    private HTable table;
    private Scan scan;
    private ResultScanner rs;
    private AtomicLong numKV = new AtomicLong();


    public MemstoreScanner(HTable table, Scan scan) {
        this.table = table;
        this.scan = scan;
        try {
            rs = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
            LOG.error("Init memstore scanner failure! MSG: " + e.getMessage());
        }
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
        Result r = rs.next();;

        if (r == null) {
            return false;
        }
        KeyValue[] kvs = r.raw();
        if (kvs.length == 0) {
            return false;
        }
        for (KeyValue kv : kvs) {
            results.add(kv);
            numKV.incrementAndGet();
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        if (rs != null) {
            rs.close();
        }
        LOG.info("Total kv number form memstore: " + numKV);
    }

}
