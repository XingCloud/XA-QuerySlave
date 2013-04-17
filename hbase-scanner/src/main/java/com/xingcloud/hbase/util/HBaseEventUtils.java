package com.xingcloud.hbase.util;

import com.xingcloud.hbase.filter.UidRangeFilter;
import com.xingcloud.hbase.filter.XARowKeyFilter;
import com.xingcloud.mongodb.MongoDBOperation;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * User: IvyTang
 * Date: 13-4-1
 * Time: 下午3:44
 */
public class HBaseEventUtils {

  private static final Log LOG = LogFactory.getLog(HBaseEventUtils.class);

  private static Log logger = LogFactory.getLog(HBaseEventUtils.class);


  public static String getEventFromDEURowKey(byte[] rowKey) {
    //DEU Uid为0xff+5个字节
    byte[] eventBytes = Arrays.copyOfRange(rowKey, 8, rowKey.length - 6);
    return Bytes.toString(eventBytes);
  }

  public static byte[] getUidOf5BytesFromDEURowKey(byte[] rowKey) {
    byte[] uid = Arrays.copyOfRange(rowKey, rowKey.length - 5, rowKey.length);
    return uid;
  }

  public static byte[] getRowKey(byte[] date, String event, byte[] uid) {

    byte[] rk = new byte[14 + event.length()];
    int index = 0;

    for (int i = 0; i < date.length; i++) {
      rk[index++] = date[i];
    }
    for (int i = 0; i < event.length(); i++) {
      rk[index++] = (byte) (event.charAt(i) & 0xFF);
    }

    //uid前加上oxff
    rk[index++] = (byte) 0xff;

    for (int i = 0; i < uid.length; i++) {
      rk[index++] = uid[i];
    }

    return rk;
  }

  public static byte[] changeRowKeyByUid(byte[] rk, byte[] newUid) {
    byte[] nrk = Arrays.copyOf(rk, rk.length);

    int j = 0;
    for (int i = nrk.length - 5; i < nrk.length; i++) {
      nrk[i] = newUid[j++];
    }
    return nrk;
  }

  public static List<String> getSortedEvents(String pID, List<String> eventFilterList) throws IOException {
      Set<String> events = new HashSet<String>();
      for (String eventFilter : eventFilterList) {
          events.addAll(MongoDBOperation.getEventSet(pID, eventFilter));
      }
      return sortEventList(new ArrayList<String>(events));
  }

  public static Pair<byte[], byte[]> getStartEndRowKey(String startDate, String endDate, List<String> sortedEvents, long startBucket, long offsetBucket) throws UnsupportedEncodingException {
    long startUid = startBucket << 32;
    long endBucket = offsetBucket + startBucket;
    long endUid = 0l;
    if (endBucket >= 256) {
      endUid = (1l << 40) - 1l;
    } else {
      endUid = endBucket << 32;
    }

    byte[] realSK = UidMappingUtil.getInstance().getRowKeyV2(startDate.replace("-", ""), sortedEvents.get(0), startUid);
    byte[] realEK = UidMappingUtil.getInstance().getRowKeyV2(endDate.replace("-", ""), sortedEvents.get(sortedEvents.size() - 1), endUid);
    return new Pair<byte[], byte[]>(realSK, realEK);
  }


  public static Filter getRowKeyFilter(List<String> sortedEvents, List<String> dates) {
      Pair<Long, Long> up = getStartEndUidPair();
      return new XARowKeyFilter(up.getFirst(), up.getSecond(), sortedEvents, dates);
  }

  public static Filter getRowKeyFilter(List<String> sortedEvents, List<String> dates, long startUid, long endUid) {
    return new XARowKeyFilter(startUid, endUid, sortedEvents, dates);
  }


  public static Pair<Long, Long> getStartEndUidPair() {
    long startUid = 0l << 32;
    long endUid = (1l << 40) - 1l;

    return new Pair<Long, Long>(startUid, endUid);
  }

  public static Pair<Long, Long> getStartEndUidPair(long startBucket, long offsetBucket) {
      long startUid = startBucket << 32;
      long endBucket = offsetBucket + startBucket;
      long endUid = 0l;
      if (endBucket >= 256) {
          endUid = (1l << 40) - 1l;
      } else {
          endUid = endBucket << 32;
      }

      return new Pair<Long, Long>(startUid, endUid);
  }

  /**
   * 按照hbase里面event的排序来排这个eventlist ,加上0xff进行字典排序。
   */
  public static List<String> sortEventList(List<String> eventList) {

    List<String> events = new ArrayList<String>();
    for (String event : eventList)
      events.add(event + String.valueOf((char) 255));
    Collections.sort(events);

    List<String> results = new ArrayList<String>();
    for (String charEvent : events)
      results.add(charEvent.substring(0, charEvent.length() - 1));

    return results;
  }

    public static long getUidOfLongFromDEURowKey(byte[] rowKey) {
        byte[] uid = new byte[8];
        int i = 0;
        for (; i < 3; i++) {
            uid[i] = 0;
        }

        for (int j = rowKey.length - 5; j < rowKey.length; j++) {
            uid[i++] = rowKey[j];
        }

        return Bytes.toLong(uid);
    }

    public static int getInnerUidFromSuid(long suid) {
        return (int)(0xffffffffl & suid);
    }

  public static  int getInnerUidFromSamplingUid(long suid) {
    return (int) (0xffffffffl & suid);
  }

}
