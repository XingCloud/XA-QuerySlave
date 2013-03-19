package com.xingcloud.hbase.meta;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-1
 * Time: 下午3:55
 * To change this template use File | Settings | File Templates.
 */
public class HBaseMeta {
    private static Logger LOG = LoggerFactory.getLogger(HBaseMeta.class);

    public static String getTablePath(String tableName, Configuration conf) {
        StringBuilder path = new StringBuilder();
        path.append(conf.get("hbase.rootdir")).append("/");
//        if (host.equals("192.168.1.147")) {
//            path.append("/hbase-web9/");
//        } else if (host.equals("192.168.1.148")) {
//            path.append("/hbase-web8/");
//        } else if (host.equals("192.168.1.150")) {
//            path.append("/hbase-web10/");
//        } else if (host.equals("192.168.1.151")) {
//            path.append("/hbase-web11");
//        } else if (host.equals("192.168.1.152")) {
//            path.append("/hbase-web12");
//        } else if (host.equals("192.168.1.154")) {
//            path.append("/hbase-web14");
//        }

        path.append(tableName).append("/");
        LOG.info("Table path: " + path.toString());
        return path.toString();
    }



}
