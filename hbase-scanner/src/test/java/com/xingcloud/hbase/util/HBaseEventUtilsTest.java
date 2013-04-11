package com.xingcloud.hbase.util;


/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-4-10
 * Time: 下午3:04
 * To change this template use File | Settings | File Templates.
 */


import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class HBaseEventUtilsTest {


  @Test
  public void testGetStartEndRowKey() {
    String startDate = "2013-01-01";
    String endDate = "2013-01-02";

    List<String> sortedEvent = getSortEvents();

    try {
      byte[] srk = UidMappingUtil.getInstance().getRowKeyV2(startDate.replace("-", ""), sortedEvent.get(0), 0);
      byte[] erk = UidMappingUtil.getInstance().getRowKeyV2(endDate.replace("-", ""), sortedEvent.get(sortedEvent.size()-1), (1l << 40) - 1l);
      Pair<byte[], byte[]> rkPair = HBaseEventUtils.getStartEndRowKey(startDate, endDate, sortedEvent, 0, 256);
      assertArrayEquals(rkPair.getFirst(), srk);
      assertArrayEquals(rkPair.getSecond(), erk);

      srk = UidMappingUtil.getInstance().getRowKeyV2(startDate.replace("-", ""), sortedEvent.get(0), 128l<<32);
      erk = UidMappingUtil.getInstance().getRowKeyV2(endDate.replace("-", ""), sortedEvent.get(sortedEvent.size()-1), (1l << 40) - 1l);
      rkPair = HBaseEventUtils.getStartEndRowKey(startDate, endDate, sortedEvent, 128, 128);
      assertArrayEquals(rkPair.getFirst(), srk);
      assertArrayEquals(rkPair.getSecond(), erk);

      srk = UidMappingUtil.getInstance().getRowKeyV2(startDate.replace("-", ""), sortedEvent.get(0), 18l<<32);
      erk = UidMappingUtil.getInstance().getRowKeyV2(endDate.replace("-", ""), sortedEvent.get(sortedEvent.size()-1), 38l<<32);
      rkPair = HBaseEventUtils.getStartEndRowKey(startDate, endDate, sortedEvent, 18, 20);
      assertArrayEquals(rkPair.getFirst(), srk);
      assertArrayEquals(rkPair.getSecond(), erk);

    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }

  private List<String> getSortEvents() {
    List<String> events = new ArrayList<String>();
    events.add("a.");
    events.add("a.b.");
    events.add("a.b.c.");
    events.add("a.b.d.");
    events.add("a.c.c.");
    events.add("c.b.a.");
    events.add("a.b.c.d.");
    return HBaseEventUtils.sortEventList(events);
  }
}
