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

    SchemaMetrics.configureGlobally(conf);
  }

  @Before
  public void clearHData() throws IOException {
    System.out.println("clear hdata,before testing...");
    //del table
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    if (hBaseAdmin.tableExists(tableName)) {
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
    hBaseAdmin.createTable(hTableDescriptor);
    IOUtils.closeStream(hBaseAdmin);
  }


  /**
   * 新插入的数据，没有flush memstore，数据只在memstore里，memonly有数据，fileonly应该为空
   *
   * @throws IOException
   */
  @Test
  public void testMemonly() throws IOException {
    //insert some rows
    HTable hTable = new HTable(conf, tableName);
    int count = 9;
    String date = "20130101";
    String nextDate = "20130102";
    String event = "visit.";
    for (int i = 0; i < count; i++) {
      long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(i);
      byte[] rowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);
      Put put = new Put(rowKey);
      put.setWriteToWAL(false);
      put.add("val".getBytes(), "val".getBytes(), System.currentTimeMillis(), Bytes.toBytes((long) i));
      hTable.put(put);
    }
    IOUtils.closeStream(hTable);

    //memonly有数据
    TableScanner memOnlyTableScanner = new TableScanner(date, nextDate, tableName, false, true);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (memOnlyTableScanner.next(results)) ;


    assertEquals(count, results.size());
    for (int i = 0; i < count; i++) {
      long value = Bytes.toLong(results.get(i).getValue());
      long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(value);
      byte[] expectRowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);

      assertEquals(Bytes.toString(expectRowKey), Bytes.toString(results.get(i).getRow()));
    }
    memOnlyTableScanner.close();

    //fileonly为空
    TableScanner fileOnlyTableScanner = new TableScanner(date, nextDate, tableName, true, false);
    results = new ArrayList<KeyValue>();
    while (fileOnlyTableScanner.next(results)) ;
    assertEquals(0, results.size());
    fileOnlyTableScanner.close();
  }


  /**
   * 新插入的数据， flush memstore之后，数据只在hfile里，memonly应该为空
   */
  @Test
  public void testFileOnly() throws IOException, InterruptedException {
    //insert some rows
    HTable hTable = new HTable(conf, tableName);
    int count = 9;
    String date = "20130101";
    String nextDate = "20130102";
    String event = "visit.";

    for (int i = 0; i < count; i++) {
      long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(i);
      byte[] rowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);
      Put put = new Put(rowKey);
      put.setWriteToWAL(false);
      put.add("val".getBytes(), "val".getBytes(), System.currentTimeMillis(), Bytes.toBytes((long) i));
      hTable.put(put);
    }
    IOUtils.closeStream(hTable);

    //flush memstore
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    hBaseAdmin.flush(tableName);
    IOUtils.closeStream(hBaseAdmin);

    //memonly should be empty
    TableScanner memOnlyTableScanner = new TableScanner(date, nextDate, tableName, false, true);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (memOnlyTableScanner.next(results)) ;
    assertEquals(0, results.size());
    memOnlyTableScanner.close();


    //hfile
    TableScanner fileOnlyTableScanner = new TableScanner(date, nextDate, tableName, true, false);
    results = new ArrayList<KeyValue>();
    fileOnlyTableScanner.next(results);
    while (fileOnlyTableScanner.next(results)) ;
    for (int i = 0; i < count; i++) {
      long value = Bytes.toLong(results.get(i).getValue());
      long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(value);
      byte[] expectRowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);
      assertEquals(Bytes.toString(expectRowKey), Bytes.toString(results.get(i).getRow()));
    }
    fileOnlyTableScanner.close();
  }


  /**
   * test uidrangefilter.
   *
   * @throws IOException
   */
  @Test
  public void testUidRangeFilterMemOnly() throws IOException, InterruptedException {
    List<String> sortEvents = getSortEvents();
    int uidFilterStopUid = 6;
    long expectResultSize = 49;

    long md5Head = HashFunctions.md5(Base64Util.toBytes(uidFilterStopUid)) & 0xff;
    int count = 9;
    String date = "20130101";
    String nextDate = "20130102";
    putFilterDataIntoHBase(sortEvents, date, count);

    long startUid = 0;
    long endUid = md5Head << 32;

    FilterList uidFilterList = HBaseEventUtils.getFilterList(null, startUid, endUid, new LinkedList<String>(sortEvents));
    TableScanner uidFilterTableScanner = new TableScanner(date, nextDate, tableName, uidFilterList, false, false);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (uidFilterTableScanner.next(results)) ;
    assertEquals(expectResultSize, results.size());

    for (KeyValue keyValue : results) {
      byte[] actualMD5UidBytes = HBaseEventUtils.getUidOf5BytesFromDEURowKey(keyValue.getRow());
      byte[] uidBytes = Arrays.copyOfRange(actualMD5UidBytes, 1, actualMD5UidBytes.length);
      byte[] intValue = Arrays.copyOfRange(keyValue.getValue(), 4, keyValue.getValue().length);
      assertArrayEquals(uidBytes, intValue);
    }
    uidFilterTableScanner.close();
  }


  /**
   * test uidrangefilter.
   *
   * @throws IOException
   */
  @Test
  public void testUidRangeFilterFileOnly() throws IOException, InterruptedException {
    List<String> sortEvents = getSortEvents();

    int uidFilterStopUid = 6;
    long expectResultSize = 49;

    long md5Head = HashFunctions.md5(Base64Util.toBytes(uidFilterStopUid)) & 0xff;
    int count = 9;
    String date = "20130101";
    String nextDate = "20130102";
    putFilterDataIntoHBase(sortEvents, date, count);

    long startUid = 0;
    long endUid = md5Head << 32;

    //flush memstore
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    hBaseAdmin.flush(tableName);
    IOUtils.closeStream(hBaseAdmin);

    FilterList uidFilterList = HBaseEventUtils.getFilterList(null, startUid, endUid, new LinkedList<String>(sortEvents));
    TableScanner uidFilterTableScanner = new TableScanner(date, nextDate, tableName, uidFilterList, false, false);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (uidFilterTableScanner.next(results)) ;
    assertEquals(expectResultSize, results.size());

    for (KeyValue keyValue : results) {
      byte[] actualMD5UidBytes = HBaseEventUtils.getUidOf5BytesFromDEURowKey(keyValue.getRow());
      byte[] uidBytes = Arrays.copyOfRange(actualMD5UidBytes, 1, actualMD5UidBytes.length);
      byte[] intValue = Arrays.copyOfRange(keyValue.getValue(), 4, keyValue.getValue().length);
      assertArrayEquals(uidBytes, intValue);
    }

    uidFilterTableScanner.close();
  }


  /**
   * Test value equal operator filter.
   *
   * @throws IOException
   */
  @Test
  public void testEQOperatorFilterMemOnly() throws IOException, InterruptedException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long equalValue = 1l;
    long startUid = 0;
    long md5Head = HashFunctions.md5(Base64Util.toBytes(3l)) & 0xff;
    long endUid = md5Head << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    Filter filter = new Filter(Constants.Operator.EQ, equalValue);
    FilterList eqFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    TableScanner eqOperatorTableScanner = new TableScanner(date, nextDate, tableName, eqFilterList, false, true);

    List<KeyValue> results = new ArrayList<KeyValue>();
    while (eqOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size(), results.size());
    for (KeyValue keyValue : results) {
      assertEquals(equalValue, Bytes.toLong(keyValue.getValue()));
    }

    eqOperatorTableScanner.close();
  }


  /**
   * Test value equal operator filter.
   *
   * @throws IOException
   */
  @Test
  public void testEQOperatorFilterFileOnly() throws IOException, InterruptedException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long equalValue = 1l;
    long startUid = 0;
    long md5Head = HashFunctions.md5(Base64Util.toBytes(3l)) & 0xff;
    long endUid = md5Head << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    Filter filter = new Filter(Constants.Operator.EQ, equalValue);
    //flush memstore
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    hBaseAdmin.flush(tableName);
    IOUtils.closeStream(hBaseAdmin);

    FilterList eqFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    TableScanner eqOperatorTableScanner = new TableScanner(date, nextDate, tableName, eqFilterList, true, false);

    List<KeyValue> results = new ArrayList<KeyValue>();
    while (eqOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size(), results.size());
    for (KeyValue keyValue : results) {
      assertEquals(equalValue, Bytes.toLong(keyValue.getValue()));
    }
    eqOperatorTableScanner.close();
  }

  /**
   * Test greater & equal operator fiters.
   *
   * @throws IOException
   */
  @Test
  public void testGEOperatorFilterMemOnly() throws IOException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long GEValue = 4l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    Filter filter = new Filter(Constants.Operator.GE, GEValue);
    FilterList geFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid, new LinkedList<String>(sortEvents));

    TableScanner geOperatorTableScanner = new TableScanner(date, nextDate, tableName, geFilterList, false, true);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (geOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - GEValue), results.size());
    geOperatorTableScanner.close();
  }

  /**
   * Test greater & equal operator fiters.
   *
   * @throws IOException
   */
  @Test
  public void testGEOperatorFilterFileOnly() throws IOException, InterruptedException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long GEValue = 4l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    //flush memstore
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    hBaseAdmin.flush(tableName);
    IOUtils.closeStream(hBaseAdmin);

    Filter filter = new Filter(Constants.Operator.GE, GEValue);
    FilterList geFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));

    TableScanner geOperatorTableScanner = new TableScanner(date, nextDate, tableName, geFilterList, true, false);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (geOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - GEValue), results.size());
    geOperatorTableScanner.close();
  }


  /**
   * Test great than operator filter.
   *
   * @throws IOException
   */
  @Test
  public void testGTOperatorFilterMemOnly() throws IOException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long GTValue = 4l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    Filter filter = new Filter(Constants.Operator.GT, GTValue);

    FilterList geFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    List<KeyValue> results = new ArrayList<KeyValue>();
    TableScanner gtOperatorTableScanner = new TableScanner(date, nextDate, tableName, geFilterList, false, true);
    while (gtOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - GTValue - 1), results.size());
    gtOperatorTableScanner.close();
  }

  /**
   * Test great than operator filter.
   *
   * @throws IOException
   */
  @Test
  public void testGTOperatorFilterFileOnly() throws IOException, InterruptedException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long GTValue = 4l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    //flush memstore
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    hBaseAdmin.flush(tableName);
    IOUtils.closeStream(hBaseAdmin);

    Filter filter = new Filter(Constants.Operator.GT, GTValue);
    FilterList geFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    List<KeyValue> results = new ArrayList<KeyValue>();
    TableScanner gtOperatorTableScanner = new TableScanner(date, nextDate, tableName, geFilterList, true, false);
    while (gtOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - GTValue - 1), results.size());
    gtOperatorTableScanner.close();
  }


  /**
   * Test less than & equal operator filter.
   *
   * @throws IOException
   */
  @Test
  public void testLEOperatorFilterMemOnly() throws IOException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long LEValue = 4l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);
    Filter filter = new Filter(Constants.Operator.LE, LEValue);

    FilterList LEFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    List<KeyValue> results = new ArrayList<KeyValue>();
    TableScanner leOperatorTableScanner = new TableScanner(date, nextDate, tableName, LEFilterList, false, true);
    while (leOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * (LEValue + 1), results.size());
    leOperatorTableScanner.close();
  }

  /**
   * Test less than & equal operator filter.
   *
   * @throws IOException
   */
  @Test
  public void testLEOperatorFilterFileOnly() throws IOException, InterruptedException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long LEValue = 4l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    //flush memstore
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    hBaseAdmin.flush(tableName);
    IOUtils.closeStream(hBaseAdmin);

    Filter filter = new Filter(Constants.Operator.LE, LEValue);
    FilterList leFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    List<KeyValue> results = new ArrayList<KeyValue>();
    TableScanner leOperatorTableScanner = new TableScanner(date, nextDate, tableName, leFilterList, true, false);
    while (leOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * (LEValue + 1), results.size());
    leOperatorTableScanner.close();
  }


  /**
   * Test less than operator filter .
   *
   * @throws IOException
   */
  @Test
  public void testLTOperatorFilterMemOnly() throws IOException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long LTValue = 4l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);
    Filter filter = new Filter(Constants.Operator.LT, LTValue);

    FilterList leFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    List<KeyValue> results = new ArrayList<KeyValue>();
    TableScanner ltOperatorTableScanner = new TableScanner(date, nextDate, tableName, leFilterList, false, true);
    while (ltOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * LTValue, results.size());
    ltOperatorTableScanner.close();
  }


  /**
   * Test less than operator filter .
   *
   * @throws IOException
   */
  @Test
  public void testLTOperatorFilterFileOnly() throws IOException, InterruptedException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long LTValue = 4l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    //flush memstore
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    hBaseAdmin.flush(tableName);
    IOUtils.closeStream(hBaseAdmin);


    Filter filter = new Filter(Constants.Operator.LT, LTValue);

    FilterList ltFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    List<KeyValue> results = new ArrayList<KeyValue>();
    TableScanner ltOperatorTableScanner = new TableScanner(date, nextDate, tableName, ltFilterList, true, false);
    while (ltOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * LTValue, results.size());
    ltOperatorTableScanner.close();

  }

  /**
   * Test not equal operator filter.
   *
   * @throws IOException
   */
  @Test
  public void testNEOperatorFilterMemOnly() throws IOException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long NEValue = 4l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    Filter filter = new Filter(Constants.Operator.NE, NEValue);

    FilterList neFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    List<KeyValue> results = new ArrayList<KeyValue>();
    TableScanner neOperatorTableScanner = new TableScanner(date, nextDate, tableName, neFilterList, false, true);
    while (neOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - 1), results.size());
    neOperatorTableScanner.close();
  }

  /**
   * Test not equal operator filter.
   *
   * @throws IOException
   */
  @Test
  public void testNEOperatorFilterFileOnly() throws IOException, InterruptedException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long NEValue = 4l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    //flush memstore
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    hBaseAdmin.flush(tableName);
    IOUtils.closeStream(hBaseAdmin);


    Filter filter = new Filter(Constants.Operator.NE, NEValue);

    FilterList neFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    List<KeyValue> results = new ArrayList<KeyValue>();
    TableScanner neOperatorTableScanner = new TableScanner(date, nextDate, tableName, neFilterList, true, false);
    while (neOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - 1), results.size());
    neOperatorTableScanner.close();
  }


  /**
   * Test between operator filter.
   *
   * @throws IOException
   */
  @Test
  public void testBetweenOperatorFilterMemOnly() throws IOException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long leftValue = 2l;
    long rightValue = 7l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    Filter filter = new Filter(Constants.Operator.BETWEEN, leftValue, rightValue);

    FilterList btFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    List<KeyValue> results = new ArrayList<KeyValue>();
    TableScanner btOperatorTableScanner = new TableScanner(date, nextDate, tableName, btFilterList, false, true);
    while (btOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * (rightValue - leftValue + 1), results.size());
    btOperatorTableScanner.close();
  }

  /**
   * Test between operator filter.
   *
   * @throws IOException
   */
  @Test
  public void testBetweenOperatorFilterFileOnly() throws IOException, InterruptedException {
    List<String> sortEvents = getSortEvents();
    int count = 9;
    long leftValue = 2l;
    long rightValue = 7l;
    long startUid = 0;
    long endUid = 255l << 32;
    String date = "20130101";
    String nextDate = "20130102";

    putFilterDataIntoHBase(sortEvents, date, count);

    //flush memstore
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    hBaseAdmin.flush(tableName);
    IOUtils.closeStream(hBaseAdmin);


    Filter filter = new Filter(Constants.Operator.BETWEEN, leftValue, rightValue);

    FilterList btFilterList = HBaseEventUtils.getFilterList(filter, startUid, endUid,
            new LinkedList<String>(sortEvents));
    List<KeyValue> results = new ArrayList<KeyValue>();
    TableScanner btOperatorTableScanner = new TableScanner(date, nextDate, tableName, btFilterList, true, false);
    while (btOperatorTableScanner.next(results)) ;
    assertEquals(sortEvents.size() * (rightValue - leftValue + 1), results.size());
    btOperatorTableScanner.close();
  }

  private void putFilterDataIntoHBase(List<String> sortEvents, String date, int count) throws IOException {
    //insert some rows
    HTable hTable = new HTable(conf, tableName);
    for (int i = 0; i < count; i++) {
      for (String event : sortEvents) {
        long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(i);
        byte[] rowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);
        Put put = new Put(rowKey);
        put.setWriteToWAL(false);
        put.add("val".getBytes(), "val".getBytes(), System.currentTimeMillis(), Bytes.toBytes((long) i));
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
