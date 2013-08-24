package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 8/22/13
 * Time: 12:53 PM
 */
public class XARegionScanner implements XAScanner{

  private MemstoresScanner memstoresScanner;
  private StoresScanner storesScanner;
  private KeyValue.KVComparator comparator;
  
  public XARegionScanner(HRegionInfo hRegionInfo, Scan scan) throws IOException {
    if(!scan.isFilesOnly()){
      memstoresScanner = new MemstoresScanner(hRegionInfo, scan);
    }
    
    if(!scan.isMemOnly()){
      storesScanner = new StoresScanner(hRegionInfo, scan);
    }
    comparator = hRegionInfo.getComparator();
    firstScan();
  }
  
  private KeyValue MSNext;
  private KeyValue SSNext;
  private KeyValue theNext;

  private void firstScan() throws IOException {
    MSNext = getKVFromMS();
    SSNext = getKVFromSS();
    theNext = getLowest(MSNext, SSNext);
  }

  public boolean next(List<KeyValue> results) throws IOException {
    KeyValue ret = theNext;
    if(theNext == MSNext){
      MSNext = getKVFromMS();
    }else{
      SSNext = getKVFromSS();
    }

    theNext = getLowest(MSNext, SSNext);
    
    if(ret != null){
      results.add(ret);
    }
    
    return ret != null;
  }

  @Override
  public void close() throws IOException {
    if(memstoresScanner != null){
      memstoresScanner.close();
    }  
    if(storesScanner != null){
      storesScanner.close();
    }
  }

  private Queue<KeyValue> MSKVCache = new LinkedList<KeyValue>();
  
  public KeyValue getKVFromMS() throws IOException {
    if (null == memstoresScanner) return null;
    
    while(true){
      if(0 == MSKVCache.size()){
        List<KeyValue> results = new ArrayList<KeyValue>();
        if(memstoresScanner.next(results)){
          MSKVCache.addAll(results);
        }else{
          return null;
        }
      }

      KeyValue kv = MSKVCache.poll();
      if(Bytes.compareTo(kv.getRow(), Bytes.toBytes("flush")) == 0){
        if(storesScanner != null){
          storesScanner.updateScanner(kv.getFamily(), theNext); //todo
          if(SSNext == null){
            SSNext = getKVFromSS();
          }
        }
      }else {
        return kv;
      }
    }
  }

  private Queue<KeyValue> SSKVCache = new LinkedList<KeyValue>();
  
  public KeyValue getKVFromSS() throws IOException {
    if(null== storesScanner) return null;
    
    if(0 == SSKVCache.size()){
      List<KeyValue> results = new ArrayList<KeyValue>();
      if(!storesScanner.next(results)){//todo when to stop
//        storesScanner.close();
//        storesScanner = null;
      }
      SSKVCache.addAll(results);
    }

    return SSKVCache.poll();
  }

  private KeyValue getLowest(final KeyValue a, final KeyValue b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return comparator.compareRows(a, b) <= 0? a: b;
  }  
}
