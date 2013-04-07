package com.xingcloud.hbase.util;

import com.xingcloud.hbase.filter.Filter;
import com.xingcloud.hbase.filter.LongComparator;
import com.xingcloud.hbase.filter.UidRangeFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

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

  public static FilterList getFilterList( Filter filter, long startUid, long endUid, Deque<String> eventQueue) {
    FilterList filterList = new FilterList();

    org.apache.hadoop.hbase.filter.Filter uidRangeFilter = new UidRangeFilter(startUid, endUid, new LinkedList<String>(eventQueue));
    filterList.addFilter(uidRangeFilter);

    if (filter != null && filter != Filter.ALL) {
      org.apache.hadoop.hbase.filter.Filter valueFilter = null;
      switch (filter.getOperator()) {
        case EQ:
          valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new LongComparator(Bytes.toBytes(filter.getValue())));
          logger.info("Init hbase value filter of " + CompareFilter.CompareOp.EQUAL + "\t" + filter.getValue() + "\t" + Bytes.toBytes(filter.getValue()));
          filterList.addFilter(valueFilter);
          break;
        case GE:
          valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new LongComparator(Bytes.toBytes(filter.getValue())));
          logger.info("Init hbase value filter of " + CompareFilter.CompareOp.GREATER_OR_EQUAL + "\t" + filter.getValue() + "\t" + Bytes.toBytes(filter.getValue()));
          filterList.addFilter(valueFilter);
          break;
        case GT:
          valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER, new LongComparator(Bytes.toBytes(filter.getValue())));
          logger.info("Init hbase value filter of " + CompareFilter.CompareOp.GREATER + "\t" + filter.getValue() + "\t" + Bytes.toBytes(filter.getValue()));
          filterList.addFilter(valueFilter);
          break;
        case LE:
          valueFilter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new LongComparator(Bytes.toBytes(filter.getValue())));
          logger.info("Init hbase value filter of " + CompareFilter.CompareOp.LESS_OR_EQUAL + "\t" + filter.getValue() + "\t" + Bytes.toBytes(filter.getValue()));
          filterList.addFilter(valueFilter);
          break;
        case LT:
          valueFilter = new ValueFilter(CompareFilter.CompareOp.LESS, new LongComparator(Bytes.toBytes(filter.getValue())));
          logger.info("Init hbase value filter of " + CompareFilter.CompareOp.LESS + "\t" + filter.getValue() + "\t" + Bytes.toBytes(filter.getValue()));
          filterList.addFilter(valueFilter);
          break;
        case NE:
          valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new LongComparator(Bytes.toBytes(filter.getValue())));
          logger.info("Init hbase value filter of " + CompareFilter.CompareOp.NOT_EQUAL + "\t" + filter.getValue() + "\t" + Bytes.toBytes(filter.getValue()));
          filterList.addFilter(valueFilter);
          break;
        case BETWEEN:
          valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new LongComparator(Bytes.toBytes(filter.getValue())));
          logger.info("Init hbase value filter of " + CompareFilter.CompareOp.GREATER_OR_EQUAL + "\t" + filter.getValue() + "\t" + Bytes.toBytes(filter.getValue()));
          filterList.addFilter(valueFilter);
          valueFilter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new LongComparator(Bytes.toBytes(filter.getExtraValue())));
          logger.info("Init hbase value filter of " + CompareFilter.CompareOp.LESS_OR_EQUAL + "\t" + filter.getValue() + "\t" + Bytes.toBytes(filter.getValue()));
          filterList.addFilter(valueFilter);
          break;
      }
      logger.info("Using filter of " + filter.toString());
    }
    return filterList;
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

}
