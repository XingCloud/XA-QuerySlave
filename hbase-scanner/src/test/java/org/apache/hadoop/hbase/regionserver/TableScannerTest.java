package org.apache.hadoop.hbase.regionserver;


import com.xingcloud.hbase.filter.Filter;
import com.xingcloud.util.Base64Util;
import com.xingcloud.util.HashFunctions;
import com.xingcloud.xa.hash.HashUtil;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import com.xingcloud.hbase.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


/**
 * User: IvyTang
 * Date: 13-4-1
 * Time: 下午4:11
 */
public class TableScannerTest {

  private String tableName = "hbasescanner_test";

  private static final Configuration conf = HBaseConfiguration.create();

  @BeforeClass
  public static void initLog4j() {
    Log4jProperties.init();
    SchemaMetrics.configureGlobally(conf);
  }


  @Before
  public void clearHData() throws IOException {
    System.out.println("clear hbase....");

    //del table
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    if (hBaseAdmin.tableExists(tableName)) {
      System.out.println("enter disableTable " + tableName);
      hBaseAdmin.disableTable(tableName);
      hBaseAdmin.deleteTable(tableName);
    }

    //create table
    HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("val");
    hColumnDescriptor.setMaxVersions(2000);
    hColumnDescriptor.setBlocksize(512 * 1024);
    hColumnDescriptor.setCompressionType(Compression.Algorithm.LZO);
    hTableDescriptor.addFamily(hColumnDescriptor);

    System.out.println("begin create table...");
    hBaseAdmin.createTable(hTableDescriptor);
    System.out.println("create table completed...");

    IOUtils.closeStream(hBaseAdmin);
  }


//    /**
//     * 新插入的数据，没有flush memstore，数据只在memstore里，memonly有数据，fileonly应该为空
//     *
//     * @throws IOException
//     */
//    @Test
//    public void testMemonly() throws IOException {
//        //insert some rows
//        HTable hTable = new HTable(conf, tableName);
//        int count = 9;
//        String date = "20130101";
//        String nextDate = "20130102";
//        String event = "visit.";
//        for (int i = 0; i < count; i++) {
//            long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(i);
//            byte[] rowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);
//            Put put = new Put(rowKey);
//            put.setWriteToWAL(false);
//            put.add("val".getBytes(), "val".getBytes(), System.currentTimeMillis(), Bytes.toBytes(i));
//            hTable.put(put);
//        }
//        IOUtils.closeStream(hTable);
//
//        //memonly有数据
//        TableScanner memOnlyTableScanner = new TableScanner(date, nextDate, tableName, false, true);
//        List<KeyValue> results = new ArrayList<KeyValue>();
//        while (memOnlyTableScanner.next(results)) ;
//
//
//        assertEquals(count, results.size());
//        for (int i = 0; i < count; i++) {
//            int value = Bytes.toInt(results.get(i).getValue());
//            long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(value);
//            byte[] expectRowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);
//            assertEquals(Bytes.toString(expectRowKey), Bytes.toString(results.get(i).getRow()));
//        }
//        memOnlyTableScanner.close();
//
//        //fileonly为空
//        TableScanner fileOnlyTableScanner = new TableScanner(date, nextDate, tableName, true, false);
//        results = new ArrayList<KeyValue>();
//        while (fileOnlyTableScanner.next(results)) ;
//        assertEquals(0, results.size());
//        fileOnlyTableScanner.close();
//    }
//
//
//    /**
//     * 新插入的数据， flush memstore之后，数据只在hfile里，memonly应该为空
//     */
//    @Test
//    public void testFileOnly() throws IOException, InterruptedException {
//        //insert some rows
//        HTable hTable = new HTable(conf, tableName);
//        int count = 9;
//        String date = "20130101";
//        String nextDate = "20130102";
//        String event = "visit.";
//
//        for (int i = 0; i < count; i++) {
//            long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(i);
//            byte[] rowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);
//            Put put = new Put(rowKey);
//            put.setWriteToWAL(false);
//            put.add("val".getBytes(), "val".getBytes(), System.currentTimeMillis(), Bytes.toBytes(i));
//            hTable.put(put);
//        }
//        IOUtils.closeStream(hTable);
//
//        //flush memstore
//        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
//        hBaseAdmin.flush(tableName);
//        IOUtils.closeStream(hBaseAdmin);
//
//        //memonly should be empty
//        TableScanner memOnlyTableScanner = new TableScanner(date, nextDate, tableName, false, true);
//        List<KeyValue> results = new ArrayList<KeyValue>();
//        while (memOnlyTableScanner.next(results)) ;
//        assertEquals(0, results.size());
//        memOnlyTableScanner.close();
//
//
//        //hfile
//        TableScanner fileOnlyTableScanner = new TableScanner(date, nextDate, tableName, true, false);
//        results = new ArrayList<KeyValue>();
//        fileOnlyTableScanner.next(results);
//        while (fileOnlyTableScanner.next(results)) ;
//        for (int i = 0; i < count; i++) {
//            int value = Bytes.toInt(results.get(i).getValue());
//            long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(value);
//            byte[] expectRowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);
//            assertEquals(Bytes.toString(expectRowKey), Bytes.toString(results.get(i).getRow()));
//        }
//        fileOnlyTableScanner.close();
//    }


//  /**
//   * test uidrangefilter.
//   *
//   * @throws IOException
//   */
//  @Test
//  public void testUidRangeFilterMemOnly() throws IOException, InterruptedException {
//    List<String> sortEvents = getSortEvents();
//
//
//    int uidFilterStopUid = 6;
//    long expectResultSize = 49;
//
//    long md5Head = HashFunctions.md5(Base64Util.toBytes(uidFilterStopUid)) & 0xff;
//    int count = 9;
//    String date = "20130101";
//    String nextDate = "20130102";
//    putFilterDataIntoHBase(sortEvents, date, count);
//
//    long startUid = 0;
//    long endUid = md5Head << 32;
//
//    FilterList uidFilterList = HBaseEventUtils.getFilterList( null, startUid, endUid, new LinkedList<String>(sortEvents));
//    TableScanner uidFilterTableScanner = new TableScanner(date, nextDate, tableName, uidFilterList, false, false);
//    List<KeyValue> results = new ArrayList<KeyValue>();
//    while (uidFilterTableScanner.next(results)) ;
//    assertEquals(expectResultSize, results.size());
//
//    for (KeyValue keyValue : results) {
//      byte[] actualMD5UidBytes = HBaseEventUtils.getUidOf5BytesFromDEURowKey(keyValue.getRow());
//      byte[] uidBytes = Arrays.copyOfRange(actualMD5UidBytes, 1, actualMD5UidBytes.length);
//      assertArrayEquals(uidBytes, keyValue.getValue());
//    }
//  }
//
//
//  /**
//   * test uidrangefilter.
//   *
//   * @throws IOException
//   */
//  @Test
//  public void testUidRangeFilterFileOnly() throws IOException, InterruptedException {
//    List<String> sortEvents = getSortEvents();
//
//    int uidFilterStopUid = 6;
//    long expectResultSize = 49;
//
//    long md5Head = HashFunctions.md5(Base64Util.toBytes(uidFilterStopUid)) & 0xff;
//    int count = 9;
//    String date = "20130101";
//    String nextDate = "20130102";
//    putFilterDataIntoHBase(sortEvents, date, count);
//
//    long startUid = 0;
//    long endUid = md5Head << 32;
//
//    //flush memstore
//    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
//    hBaseAdmin.flush(tableName);
//    IOUtils.closeStream(hBaseAdmin);
//
//
//    FilterList uidFilterList = HBaseEventUtils.getFilterList(null, startUid, endUid, new LinkedList<String>(sortEvents));
//    TableScanner uidFilterTableScanner = new TableScanner(date, nextDate, tableName, uidFilterList, false, false);
//    List<KeyValue> results = new ArrayList<KeyValue>();
//    while (uidFilterTableScanner.next(results)) ;
//        assertEquals(expectResultSize, results.size());
//
//    for (KeyValue keyValue : results) {
//            byte[] actualMD5UidBytes = HBaseEventUtils.getUidOf5BytesFromDEURowKey(keyValue.getRow());
//            byte[] uidBytes = Arrays.copyOfRange(actualMD5UidBytes, 1, actualMD5UidBytes.length);
//            assertArrayEquals(uidBytes, keyValue.getValue());
//
//    }
//  }





    /**
     * Test value equal operator filter.
     *
     * @throws IOException
     */
    @Test
    public void testEQOperatorFilter() throws IOException {
        List<String> sortEvents = getSortEvents();
        int count = 9;
        int equalValue = 5;
        String keyDatePrefix = "20130101";
        putFilterDataIntoHBase(sortEvents, keyDatePrefix, count);

        Filter filter = new Filter(Constants.Operator.EQ, equalValue);
        FilterList eqFilterList = HBaseEventUtils.getFilterList(filter, 0, count - 1, new LinkedList<String>(sortEvents));
        TableScanner eqOperatorTableScanner = new TableScanner(sortEvents.get(0).concat("0"),
                sortEvents.get(sortEvents.size() - 1).concat(String.valueOf(count)), tableName, eqFilterList,
                true, true);
        List<KeyValue> results = new ArrayList<KeyValue>();
        eqOperatorTableScanner.next(results);
        assertEquals(sortEvents.size(), results.size());

        for (int i = 0; i < sortEvents.size(); i++) {
            String event = sortEvents.get(i);
            KeyValue keyValue = results.get(i);
            assertEquals(5, Bytes.toInt(keyValue.getValue()));
            assertEquals(keyDatePrefix + event + equalValue, Bytes.toString(keyValue.getRow()));
        }
    }

//    /**
//     * Test greater & equal operator fiters.
//     *
//     * @throws IOException
//     */
//    @Test
//    public void testGEOperatorFilter() throws IOException {
//        List<String> sortEvents = getSortEvents();
//        int count = 9;
//        int GEValue = 5;
//        String keyDatePrefix = "20130101";
//        putFilterDataIntoHBase(sortEvents, keyDatePrefix, count);
//        Filter filter = new Filter(Constants.Operator.GE, GEValue);
//        FilterList geFilterList = HBaseEventUtils.getFilterList(null, filter, 0, count - 1,
//                new LinkedList<String>(sortEvents));
//        TableScanner geOperatorTableScanner = new TableScanner(sortEvents.get(0).concat("0"),
//                sortEvents.get(sortEvents.size() - 1).concat(String.valueOf(count)), tableName, geFilterList,
//                true, true);
//        List<KeyValue> results = new ArrayList<KeyValue>();
//        geOperatorTableScanner.next(results);
//        assertEquals(sortEvents.size() * (count - GEValue + 1), results.size());
//
//        int index = 0;
//        for (String event : sortEvents) {
//            for (int value = GEValue; value < count; value++) {
//                KeyValue keyValue = results.get(index);
//                assertEquals(value, Bytes.toInt(keyValue.getValue()));
//                assertEquals(keyDatePrefix + event + value, Bytes.toString(keyValue.getRow()));
//                index++;
//            }
//        }
//    }
//
//    /**
//     * Test great than operator filter.
//     *
//     * @throws IOException
//     */
//    @Test
//    public void testGTOperatorFilter() throws IOException {
//        List<String> sortEvents = getSortEvents();
//        int count = 9;
//        int GTValue = 5;
//        String keyDatePrefix = "20130101";
//        putFilterDataIntoHBase(sortEvents, keyDatePrefix, count);
//        Filter filter = new Filter(Constants.Operator.GT, GTValue);
//        FilterList gtFilterList = HBaseEventUtils.getFilterList(null, filter, 0, count - 1,
//                new LinkedList<String>(sortEvents));
//        TableScanner gtOperatorTableScanner = new TableScanner(sortEvents.get(0).concat("0"),
//                sortEvents.get(sortEvents.size() - 1).concat(String.valueOf(count)), tableName, gtFilterList,
//                true, true);
//        List<KeyValue> results = new ArrayList<KeyValue>();
//        gtOperatorTableScanner.next(results);
//        assertEquals(sortEvents.size() * (count - GTValue), results.size());
//
//        int index = 0;
//        for (String event : sortEvents) {
//            for (int value = GTValue + 1; value < count; value++) {
//                KeyValue keyValue = results.get(index);
//                assertEquals(value, Bytes.toInt(keyValue.getValue()));
//                assertEquals(keyDatePrefix + event + value, Bytes.toString(keyValue.getRow()));
//                index++;
//            }
//        }
//    }
//
//
//    /**
//     * Test less than & equal operator filter.
//     *
//     * @throws IOException
//     */
//    @Test
//    public void testLEOperatorFilter() throws IOException {
//        List<String> sortEvents = getSortEvents();
//        int count = 9;
//        int LEValue = 5;
//        String keyDatePrefix = "20130101";
//        putFilterDataIntoHBase(sortEvents, keyDatePrefix, count);
//        Filter filter = new Filter(Constants.Operator.LE, LEValue);
//        FilterList leFilterList = HBaseEventUtils.getFilterList(null, filter, 0, count - 1,
//                new LinkedList<String>(sortEvents));
//        TableScanner leOperatorTableScanner = new TableScanner(sortEvents.get(0).concat("0"),
//                sortEvents.get(sortEvents.size() - 1).concat(String.valueOf(count)), tableName, leFilterList,
//                true, true);
//        List<KeyValue> results = new ArrayList<KeyValue>();
//        leOperatorTableScanner.next(results);
//        assertEquals(sortEvents.size() * (LEValue + 1), results.size());
//
//        int index = 0;
//        for (String event : sortEvents) {
//            for (int value = 0; value < LEValue + 1; value++) {
//                KeyValue keyValue = results.get(index);
//                assertEquals(value, Bytes.toInt(keyValue.getValue()));
//                assertEquals(keyDatePrefix + event + value, Bytes.toString(keyValue.getRow()));
//                index++;
//            }
//        }
//    }
//
//
//    /**
//     * Test less than operator filter .
//     *
//     * @throws IOException
//     */
//    @Test
//    public void testLTOperatorFilter() throws IOException {
//        List<String> sortEvents = getSortEvents();
//        int count = 9;
//        int LTValue = 5;
//        String keyDatePrefix = "20130101";
//        putFilterDataIntoHBase(sortEvents, keyDatePrefix, count);
//
//        Filter filter = new Filter(Constants.Operator.LT, LTValue);
//        FilterList ltFilterList = HBaseEventUtils.getFilterList(null, filter, 0, count - 1,
//                new LinkedList<String>(sortEvents));
//        TableScanner ltOperatorTableScanner = new TableScanner(sortEvents.get(0).concat("0"),
//                sortEvents.get(sortEvents.size() - 1).concat(String.valueOf(count)), tableName, ltFilterList,
//                true, true);
//        List<KeyValue> results = new ArrayList<KeyValue>();
//        ltOperatorTableScanner.next(results);
//        assertEquals(sortEvents.size() * LTValue, results.size());
//
//        int index = 0;
//        for (String event : sortEvents) {
//            for (int value = 0; value < LTValue; value++) {
//                KeyValue keyValue = results.get(index);
//                assertEquals(value, Bytes.toInt(keyValue.getValue()));
//                assertEquals(keyDatePrefix + event + value, Bytes.toString(keyValue.getRow()));
//                index++;
//            }
//        }
//    }
//
//    /**
//     * Test not equal operator filter.
//     *
//     * @throws IOException
//     */
//    @Test
//    public void testNEOperatorFilter() throws IOException {
//        List<String> sortEvents = getSortEvents();
//        int count = 9;
//        int NEValue = 5;
//        String keyDatePrefix = "20130101";
//        putFilterDataIntoHBase(sortEvents, keyDatePrefix, count);
//
//        Filter neFilter = new Filter(Constants.Operator.NE, NEValue);
//        FilterList neFilterList = HBaseEventUtils.getFilterList(null, neFilter, 0, count - 1,
//                new LinkedList<String>(sortEvents));
//        TableScanner NEOperatorTableScanner = new TableScanner(sortEvents.get(0).concat("0"),
//                sortEvents.get(sortEvents.size() - 1).concat(String.valueOf(count)), tableName, neFilterList,
//                true, true);
//        List<KeyValue> results = new ArrayList<KeyValue>();
//        NEOperatorTableScanner.next(results);
//        assertEquals(sortEvents.size() * (count - 1), results.size());
//
//        int index = 0;
//        for (String event : sortEvents) {
//            for (int value = 0; value < count; value++) {
//                if (value == NEValue)
//                    continue;
//                KeyValue keyValue = results.get(index);
//                assertEquals(value, Bytes.toInt(keyValue.getValue()));
//                assertEquals(keyDatePrefix + event + value, Bytes.toString(keyValue.getRow()));
//                index++;
//            }
//        }
//    }
//
//
//    /**
//     * Test between operator filter.
//     *
//     * @throws IOException
//     */
//    @Test
//    public void testBetweenOperatorFilter() throws IOException {
//        List<String> sortEvents = getSortEvents();
//        int count = 9;
//        int leftValue = 5;
//        int rightValue = 7;
//        String keyDatePrefix = "20130101";
//        putFilterDataIntoHBase(sortEvents, keyDatePrefix, count);
//
//        Filter betweenFilter = new Filter(Constants.Operator.BETWEEN, leftValue, rightValue);
//        FilterList betweenFilterList = HBaseEventUtils.getFilterList(null, betweenFilter, 0, count - 1,
//                new LinkedList<String>(sortEvents));
//        TableScanner betweenTableScanner = new TableScanner(sortEvents.get(0).concat("0"),
//                sortEvents.get(sortEvents.size() - 1).concat(String.valueOf(count)), tableName, betweenFilterList,
//                true, true);
//        List<KeyValue> results = new ArrayList<KeyValue>();
//        betweenTableScanner.next(results);
//        assertEquals(sortEvents.size() * (rightValue - leftValue + 1), results.size());
//
//        int index = 0;
//        for (String event : sortEvents) {
//            for (int value = leftValue; value <= rightValue; value++) {
//                KeyValue keyValue = results.get(index);
//                assertEquals(value, Bytes.toInt(keyValue.getValue()));
//                assertEquals(keyDatePrefix + event + value, Bytes.toString(keyValue.getRow()));
//                index++;
//            }
//        }
//    }
//
  private void putFilterDataIntoHBase(List<String> sortEvents, String date, int count) throws IOException {
    //insert some rows
    HTable hTable = new HTable(conf, tableName);
    for (int i = 0; i < count; i++) {
      for (String event : sortEvents) {
        long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(i);
        byte[] rowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);
        Put put = new Put(rowKey);
        put.setWriteToWAL(false);
        put.add("val".getBytes(), "val".getBytes(), System.currentTimeMillis(), Bytes.toBytes(i));
        hTable.put(put);
      }
    }
    IOUtils.closeStream(hTable);
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
