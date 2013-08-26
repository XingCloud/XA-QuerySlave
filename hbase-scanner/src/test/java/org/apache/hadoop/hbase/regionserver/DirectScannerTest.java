package org.apache.hadoop.hbase.regionserver;


import com.xingcloud.hbase.filter.LongComparator;
import com.xingcloud.util.Base64Util;
import com.xingcloud.util.HashFunctions;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ValueFilter;
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
public class DirectScannerTest {

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
    DirectScanner memOnlyDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, false, true);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (memOnlyDirectScanner.next(results)) ;


    assertEquals(count, results.size());
    for (int i = 0; i < count; i++) {
      long value = Bytes.toLong(results.get(i).getValue());
      long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(value);
      byte[] expectRowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);

      assertEquals(Bytes.toString(expectRowKey), Bytes.toString(results.get(i).getRow()));
    }
    memOnlyDirectScanner.close();

    //fileonly为空
    DirectScanner fileOnlyDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, true, false);
    results = new ArrayList<KeyValue>();
    while (fileOnlyDirectScanner.next(results)) ;
    assertEquals(0, results.size());
    fileOnlyDirectScanner.close();
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
    DirectScanner memOnlyDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, false, true);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (memOnlyDirectScanner.next(results)) ;
    assertEquals(0, results.size());
    memOnlyDirectScanner.close();


    //hfile
    DirectScanner fileOnlyDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, true, false);
    results = new ArrayList<KeyValue>();
    fileOnlyDirectScanner.next(results);
    while (fileOnlyDirectScanner.next(results)) ;
    for (int i = 0; i < count; i++) {
      long value = Bytes.toLong(results.get(i).getValue());
      long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(value);
      byte[] expectRowKey = UidMappingUtil.getInstance().getRowKeyV2(date, event, md5Uid);
      assertEquals(Bytes.toString(expectRowKey), Bytes.toString(results.get(i).getRow()));
    }
    fileOnlyDirectScanner.close();
  }

  /**
   * test halfway flush
   */
  
  @Test
  public void testHalfwayFlush() throws IOException, InterruptedException {
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
    DirectScanner directScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, false, false);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (directScanner.next(results)){
      //flush memstore
      HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
      hBaseAdmin.flush(tableName);
      IOUtils.closeStream(hBaseAdmin);        
    }

    for (KeyValue kv:results){
      System.out.println(Bytes.toLong(kv.getValue()));
    }
    assertEquals(9, results.size());
  }
  
  @Test
  public void testBothFileAndMem() throws IOException, InterruptedException {
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
      if(i==4){
        //flush memstore
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        hBaseAdmin.flush(tableName);
        IOUtils.closeStream(hBaseAdmin);
      }
    }
    IOUtils.closeStream(hTable);
    
    //we have 4 kv in the memstore
    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner memScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, false, true);
    while (memScanner.next(results));
    for (KeyValue kv:results){
      System.out.println(Bytes.toLong(kv.getValue()));
    }
    assertEquals(4, results.size());
    System.out.println("==============");
    
    //we have 5 kv in the storefile
    results.clear();
    DirectScanner fileScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, true, false);
    while (fileScanner.next(results));    
    for (KeyValue kv:results){
      System.out.println(Bytes.toLong(kv.getValue()));
    }
    assertEquals(5, results.size());
    
    //get all kv
    System.out.println("==============");
    results.clear();
    DirectScanner directScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, false, false);
    while (directScanner.next(results));
    for (KeyValue kv:results){
      System.out.println(Bytes.toLong(kv.getValue()));
    }
    assertEquals(9, results.size());
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


    FilterList eqFilterList = new FilterList();
    Filter valFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new LongComparator(Bytes.toBytes(equalValue)));
    eqFilterList.addFilter(valFilter);

    DirectScanner eqOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, eqFilterList, false, true);

    List<KeyValue> results = new ArrayList<KeyValue>();
    while (eqOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size(), results.size());
    for (KeyValue keyValue : results) {
      assertEquals(equalValue, Bytes.toLong(keyValue.getValue()));
    }

    eqOperatorDirectScanner.close();
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


    //flush memstore
    HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
    hBaseAdmin.flush(tableName);
    IOUtils.closeStream(hBaseAdmin);

    FilterList eqFilterList = new FilterList();
    Filter valFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new LongComparator(Bytes.toBytes(equalValue)));
    eqFilterList.addFilter(valFilter);

    DirectScanner eqOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, eqFilterList, true, false);

    List<KeyValue> results = new ArrayList<KeyValue>();
    while (eqOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size(), results.size());
    for (KeyValue keyValue : results) {
      assertEquals(equalValue, Bytes.toLong(keyValue.getValue()));
    }
    eqOperatorDirectScanner.close();
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

    FilterList geFilterList = new FilterList();
    Filter filter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new LongComparator(Bytes.toBytes(GEValue)));
    geFilterList.addFilter(filter);

    DirectScanner geOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, geFilterList, false, true);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (geOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - GEValue), results.size());
    geOperatorDirectScanner.close();
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

    FilterList geFilterList = new FilterList();
    Filter filter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new LongComparator(Bytes.toBytes(GEValue)));
    geFilterList.addFilter(filter);

    DirectScanner geOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, geFilterList, true, false);
    List<KeyValue> results = new ArrayList<KeyValue>();
    while (geOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - GEValue), results.size());
    geOperatorDirectScanner.close();
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


    FilterList gtFilterList = new FilterList();
    Filter valFilter = new ValueFilter(CompareFilter.CompareOp.GREATER, new LongComparator(Bytes.toBytes(GTValue)));
    gtFilterList.addFilter(valFilter);

    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner gtOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, gtFilterList, false, true);
    while (gtOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - GTValue - 1), results.size());
    gtOperatorDirectScanner.close();
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


    FilterList geFilterList = new FilterList();
    Filter valFilter = new ValueFilter(CompareFilter.CompareOp.GREATER, new LongComparator(Bytes.toBytes(GTValue)));
    geFilterList.addFilter(valFilter);

    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner gtOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, geFilterList, true, false);
    while (gtOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - GTValue - 1), results.size());
    gtOperatorDirectScanner.close();
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

    FilterList leFilterList = new FilterList();
    Filter valFilter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new LongComparator(Bytes.toBytes(LEValue)));
    leFilterList.addFilter(valFilter);

    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner leOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, leFilterList, false, true);
    while (leOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * (LEValue + 1), results.size());
    leOperatorDirectScanner.close();
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

    FilterList leFilterList = new FilterList();
    Filter valFilter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new LongComparator(Bytes.toBytes(LEValue)));
    leFilterList.addFilter(valFilter);

    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner leOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, leFilterList, true, false);
    while (leOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * (LEValue + 1), results.size());
    leOperatorDirectScanner.close();
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

    FilterList ltFilterList = new FilterList();
    Filter valFilter = new ValueFilter(CompareFilter.CompareOp.LESS, new LongComparator(Bytes.toBytes(LTValue)));
    ltFilterList.addFilter(valFilter);

    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner ltOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, ltFilterList, false, true);
    while (ltOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * LTValue, results.size());
    ltOperatorDirectScanner.close();
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


    FilterList ltFilterList = new FilterList();
    Filter valFilter = new ValueFilter(CompareFilter.CompareOp.LESS, new LongComparator(Bytes.toBytes(LTValue)));
    ltFilterList.addFilter(valFilter);

    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner ltOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, ltFilterList, true, false);
    while (ltOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * LTValue, results.size());
    ltOperatorDirectScanner.close();

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


    FilterList neFilterList = new FilterList();
    Filter valFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new LongComparator(Bytes.toBytes(NEValue)));
    neFilterList.addFilter(valFilter);

    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner neOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, neFilterList, false, true);
    while (neOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - 1), results.size());
    neOperatorDirectScanner.close();
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


    FilterList neFilterList = new FilterList();
    Filter valFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new LongComparator(Bytes.toBytes(NEValue)));
    neFilterList.addFilter(valFilter);

    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner neOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, neFilterList, true, false);
    while (neOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * (count - 1), results.size());
    neOperatorDirectScanner.close();
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


    FilterList btFilterList = new FilterList();
    Filter leftValueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new LongComparator(Bytes.toBytes(leftValue)));
    Filter rightValueFilter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new LongComparator(Bytes.toBytes(rightValue)));
    btFilterList.addFilter(leftValueFilter);
    btFilterList.addFilter(rightValueFilter);

    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner btOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, btFilterList, false, true);
    while (btOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * (rightValue - leftValue + 1), results.size());
    btOperatorDirectScanner.close();
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


    FilterList btFilterList = new FilterList();
    Filter leftValueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new LongComparator(Bytes.toBytes(leftValue)));
    Filter rightValueFilter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new LongComparator(Bytes.toBytes(rightValue)));
    btFilterList.addFilter(leftValueFilter);
    btFilterList.addFilter(rightValueFilter);

    List<KeyValue> results = new ArrayList<KeyValue>();
    DirectScanner btOperatorDirectScanner = new DirectScanner(Bytes.toBytes(date), Bytes.toBytes(nextDate), tableName, btFilterList, true, false);
    while (btOperatorDirectScanner.next(results)) ;
    assertEquals(sortEvents.size() * (rightValue - leftValue + 1), results.size());
    btOperatorDirectScanner.close();
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
