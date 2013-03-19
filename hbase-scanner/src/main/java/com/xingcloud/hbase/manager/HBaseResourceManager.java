package com.xingcloud.hbase.manager;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class HBaseResourceManager {
    private static Logger logger = LoggerFactory.getLogger(HBaseResourceManager.class);
    private static Configuration conf;
    private HTablePool pool;
    private final int max_size = 200;
    private static HBaseResourceManager m_instance;
    
    static {
        conf = HBaseConfiguration.create();
    }
    
    public synchronized static HBaseResourceManager getInstance() throws IOException {
        if (m_instance == null) {
            m_instance = new HBaseResourceManager();
        }
        return m_instance; 
    }
    
    
    private HBaseResourceManager() throws IOException {
        this.pool = new HTablePool(conf, max_size);
    }
    
    public HTable getTable(byte[] tableName) throws IOException {
        return (HTable) pool.getTable(tableName);
    }
    
    public HTable getTable(String tableName) throws IOException {
        HTable htable = null;
        try {
            htable = (HTable) pool.getTable(tableName);
        } catch (Exception e) {
            logger.error("Table not found. " + tableName);
            throw new IOException("Table not found. " + tableName);
        }
        return htable;
    }
    
    public void putTable(HTable htable) throws IOException {
        if (htable != null) {
            htable.close();
        }
    }
    
    public void closeAll() throws IOException {
        this.pool.close();
    }
    
    public void closeAll(String projectId) throws IOException {
        this.pool.closeTablePool(projectId + "_deu"); 
    }
    
    public void closeAllConnections() {
       HConnectionManager.deleteAllConnections(true);
    }
}
