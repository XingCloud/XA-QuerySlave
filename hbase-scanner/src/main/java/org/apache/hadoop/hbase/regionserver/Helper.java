package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 8/22/13
 * Time: 9:45 AM
 */
public class Helper {

  private static Logger LOG = LoggerFactory.getLogger(Helper.class);
  public static List<HRegionInfo> getRegionInfoList(HTable hTable, Pair<byte[], byte[]> seKey) throws IOException {
    Set<HRegionInfo> hRegionInfoSet = new HashSet<HRegionInfo>();
    NavigableMap<HRegionInfo, ServerName> regionInfoMap = hTable.getRegionLocations();
    if (regionInfoMap.size() == 1) {
      /*There is only one region*/
      return new ArrayList<HRegionInfo>(regionInfoMap.keySet());
    }
    List<HRegionInfo> regionInfoList = getRegionInfoList(hTable, seKey.getFirst(), seKey.getSecond());
    if (regionInfoList.size() == 0) {
      throw new IOException("Can't get region info of " + hTable.getTableName());
    }

    StringBuilder summary = new StringBuilder("Calling region list:\n");
    for (HRegionInfo ri : regionInfoList) {
      summary.append(ri.getRegionNameAsString()).append("\n");
    }
    LOG.info(summary.toString());
    return regionInfoList;
  }

  public static List<HRegionInfo> getRegionInfoList(HTable hTable, byte[] startRowKey, byte[] endRowKey) throws IOException {
    NavigableMap<HRegionInfo, ServerName> regionInfoMap = hTable.getRegionLocations();

    Map<ServerName, List<HRegionInfo>> counterMap = new HashMap<ServerName, List<HRegionInfo>>();

    Set<HRegionInfo> regionInfoSet = regionInfoMap.keySet();
    List<HRegionInfo> regionInfoList = new ArrayList<HRegionInfo>();

    for (HRegionInfo hRegionInfo : regionInfoSet) {
      byte[] startKey = hRegionInfo.getStartKey();
      byte[] endKey = hRegionInfo.getEndKey();
      if (Bytes.compareTo(startRowKey, startKey) >= 0) {
        if (Bytes.compareTo(startRowKey, endKey) < 0 || Bytes.equals(endKey, HConstants.EMPTY_END_ROW)) {
          regionInfoList.add(hRegionInfo);
          ServerName sn = regionInfoMap.get(hRegionInfo);
          if (counterMap.containsKey(sn)) {
            List<HRegionInfo> list = counterMap.get(sn);
            list.add(hRegionInfo);
            counterMap.put(sn, list);
          } else {
            List<HRegionInfo> list = new ArrayList<HRegionInfo>();
            list.add(hRegionInfo);
            counterMap.put(sn, list);
          }
        }
      } else if (Bytes.equals(endRowKey, HConstants.EMPTY_END_ROW) || Bytes.compareTo(startKey, endRowKey) <= 0) {
        regionInfoList.add(hRegionInfo);
        ServerName sn = regionInfoMap.get(hRegionInfo);
        if (counterMap.containsKey(sn)) {
          List<HRegionInfo> list = counterMap.get(sn);
          list.add(hRegionInfo);
          counterMap.put(sn, list);
        } else {
          List<HRegionInfo> list = new ArrayList<HRegionInfo>();
          list.add(hRegionInfo);
          counterMap.put(sn, list);
        }
      }

    }

    return regionInfoList;
  }
}
