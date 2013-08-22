package org.apache.hadoop.hbase.regionserver;

import com.xingcloud.hbase.manager.HBaseResourceManager;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-7
 * Time: 下午5:07
 * To change this template use File | Settings | File Templates.
 */
public class DirectScanner implements XAScanner {
  private static Logger LOG = LoggerFactory.getLogger(DirectScanner.class);

  private byte[] startRowKey;
  private byte[] endRowKey;
  private String tableName;
  private Filter filter;

  private List<XAScanner> scanners;

  boolean isMemOnly = false;
  boolean isFileOnly = false;
  private int currentIndex = 0;
  private List<HRegionInfo> regionList;
  private XAScanner currentScanner;
  private boolean hasNext;
  private Scan scan;
  
  public DirectScanner(byte[] startRowKey, byte[] endRowKey, String tableName, Filter filter,
                       boolean isFileOnly, boolean isMemOnly) throws IOException {
    this.isFileOnly = isFileOnly;
    this.isMemOnly = isMemOnly;
    this.startRowKey = startRowKey;
    this.endRowKey = endRowKey;
    this.tableName = tableName;
    this.filter = filter;

    //set scan
    this.scan = new Scan(startRowKey, endRowKey);
    scan.setMemOnly(isMemOnly);
    scan.setFilesOnly(isFileOnly);
    if (filter != null)
      scan.setFilter(filter);
    scan.addColumn(Bytes.toBytes("val"), Bytes.toBytes("val"));
    
    // get regions 
    Pair<byte[], byte[]> seKey = new Pair(startRowKey, endRowKey);
    HTable table = HBaseResourceManager.getInstance().getTable(Bytes.toBytes(tableName));
    this.regionList = Helper.getRegionInfoList(table, seKey);
    LOG.info("Number of regions: " + regionList.size() + " for " + tableName + " " + startRowKey + " " + endRowKey);
  }

  @Override
  public boolean next(List<KeyValue> results) throws IOException {
    if (regionList.size() == 0)
      return false;
    
    checkScanner(); // check if we should advance to next region 
    
    if(currentScanner == null){
      return false;
    }
    
    hasNext = currentScanner.next(results);
    
    return true;
  }

  private boolean checkScanner() throws IOException {
    if(currentScanner == null){
      currentScanner = new XARegionScanner(regionList.get(0), scan);
    }else{
      if (! hasNext){
        currentScanner.close();
        currentIndex++; 
        if (currentIndex > regionList.size()-1){
          currentScanner = null;
          return false;
        }
        currentScanner = new StoresScanner(regionList.get(currentIndex), scan);
      }
    }
    
    return true;
  }
  
  @Override
  public void close() throws IOException {
    for (XAScanner regionScanner : scanners) {
      if (regionScanner != null) {
        regionScanner.close();
      }
    }
  }

  public static void main(String[] args) throws IOException {
    String tableName = args[0];
    String srk = args[1];
    String erk = args[2];
    boolean isMemOnly = Boolean.parseBoolean(args[3]);
    boolean isFileOnly = Boolean.parseBoolean(args[4]);
    DirectScanner scanner = new DirectScanner(Bytes.toBytes(srk), Bytes.toBytes(erk), tableName, null, isMemOnly, isFileOnly);
    long counter = 0;
    long st = System.nanoTime();
    List<KeyValue> results = new ArrayList<KeyValue>();
    boolean done = false;
    try {
      do {
        results.clear();
        done = scanner.next(results);
        for (KeyValue kv : results) {
          if (counter % 1000 == 0 || !done) {
            LOG.info(Bytes.toString(kv.getRow()));
          }
          counter++;
        }

      } while (done);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (scanner != null) {
        try {
          scanner.close();
        } catch (IOException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }
    LOG.info("Scan finish. Total rows: " + counter + " Taken: " + (System.nanoTime() - st) / 1.0e9 + " sec");
  }
}
