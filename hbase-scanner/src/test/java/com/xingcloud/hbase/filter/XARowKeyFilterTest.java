package com.xingcloud.hbase.filter;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-4-12
 * Time: 下午3:48
 * To change this template use File | Settings | File Templates.
 */
import com.xingcloud.hbase.util.HBaseEventUtils;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.DirectScanner;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.IOUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class XARowKeyFilterTest {
    private Log logger = LogFactory.getLog(XARowKeyFilterTest.class);

    private static int totalUidNum = 300;
    long startDate = 20130101;
    long endDate = 20130103;
    private static List<String> events = new ArrayList<String>();
    private static List<String> dates = new ArrayList<String>();

    private static String tableName = "hbase_filter_test";

    private static final Configuration conf = HBaseConfiguration.create();

    @BeforeClass
    public static void initHBaseTable() {
        SchemaMetrics.configureGlobally(conf);
        events.add("visit.");
        events.add("visit.a1.");
        events.add("visit.a2.");
        events.add("visit.a3.");
        events.add("visit.a1.b1.");
        events.add("visit.a1.b2.");
        events.add("visit.a1.b1.c1.");

        dates.add("20130101");
        dates.add("20130102");
        dates.add("20130103");

        try {
            dropTable();
            initHBaseFilterdata();
            flushTable();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    @Test
    public void testFilterEvent1() {
        System.out.println("Test skip event visit.a1.b1.c1.");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);
            List<String> testEventSet1 = new ArrayList<String>(sortedEvents);
            testEventSet1.remove(0);
            Pair<byte[], byte[]> pair = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), testEventSet1, 0, 256);
            Filter filter = HBaseEventUtils.getRowKeyFilter(testEventSet1, dates);
            DirectScanner scanner = new DirectScanner(pair.getFirst(), pair.getSecond(), tableName, filter, false, false);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (scanner.next(results));

            assertEquals(5400, results.size());

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void testFilterEvent2() {
        System.out.println("Test skip event " + "visit.a1.b1.");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);
            List<String> testEventSet2 = new ArrayList<String>(sortedEvents);
            testEventSet2.remove(1);
            Pair<byte[], byte[]> pair2 = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), testEventSet2, 0, 256);
            Filter filter2 = HBaseEventUtils.getRowKeyFilter(testEventSet2, dates);
            DirectScanner scanner2 = new DirectScanner(pair2.getFirst(), pair2.getSecond(), tableName, filter2, false, false);
            List<KeyValue> results2 = new ArrayList<KeyValue>();
            while (scanner2.next(results2));
            assertEquals(5400, results2.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFilterEvent3() {
        System.out.println("Test skip event " + "visit.");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);
            List<String> testEventSet3 = new ArrayList<String>(sortedEvents);
            testEventSet3.remove(sortedEvents.size()-1);
            Pair<byte[], byte[]> pair3 = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), testEventSet3, 0, 256);
            Filter filter2 = HBaseEventUtils.getRowKeyFilter(testEventSet3, dates);
            DirectScanner scanner3 = new DirectScanner(pair3.getFirst(), pair3.getSecond(), tableName, filter2, false, false);
            List<KeyValue> results2 = new ArrayList<KeyValue>();
            while (scanner3.next(results2));
            assertEquals(5400, results2.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testFilterDate1() {
        System.out.println("Test skip date 20130101");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);
            List<String> testDates1 = new ArrayList<String>(dates);
            testDates1.remove(0);
            Pair<byte[], byte[]> pair = HBaseEventUtils.getStartEndRowKey(String.valueOf(testDates1.get(0)), String.valueOf(endDate), sortedEvents, 0, 256);
            Filter filter = HBaseEventUtils.getRowKeyFilter(sortedEvents, testDates1);
            DirectScanner scanner = new DirectScanner(pair.getFirst(), pair.getSecond(), tableName, filter, false, false);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (scanner.next(results));
            assertEquals(4200, results.size());

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void testFilterDate2() {
        System.out.println("Test skip date 20130102");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);
            List<String> testDates1 = new ArrayList<String>(dates);
            testDates1.remove(1);
            Pair<byte[], byte[]> pair = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), sortedEvents, 0, 256);
            Filter filter = HBaseEventUtils.getRowKeyFilter(sortedEvents, testDates1);
            DirectScanner scanner = new DirectScanner(pair.getFirst(), pair.getSecond(), tableName, filter, false, false);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (scanner.next(results));
            assertEquals(4200, results.size());
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void testFilterUid1() {
        System.out.println("Test filter uid. Bucket: 1");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);
            Pair<byte[], byte[]> pair = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), sortedEvents, 0, 1);
            Pair<Long, Long> uidPair = HBaseEventUtils.getStartEndUidPair(0, 1);
            Filter filter = HBaseEventUtils.getRowKeyFilter(sortedEvents, dates, uidPair.getFirst(), uidPair.getSecond());
            DirectScanner scanner = new DirectScanner(pair.getFirst(), pair.getSecond(), tableName, filter, false, false);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (scanner.next(results));
            assertEquals(42, results.size());
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void testFilterUid2() {
        System.out.println("Test filter uid. Bucket: 128");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);

            Pair<byte[], byte[]> pair = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), sortedEvents, 127, 1);

            Pair<Long, Long> uidPair = HBaseEventUtils.getStartEndUidPair(127, 1);
            Filter filter = HBaseEventUtils.getRowKeyFilter(sortedEvents, dates, uidPair.getFirst(), uidPair.getSecond());
            DirectScanner scanner = new DirectScanner(pair.getFirst(), pair.getSecond(), tableName, filter, false, false);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (scanner.next(results));

            assertEquals(21, results.size());

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void testFilterUid3() {
        System.out.println("Test filter uid. Bucket: 255");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);

            Pair<byte[], byte[]> pair = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), sortedEvents, 255, 1);

            Pair<Long, Long> uidPair = HBaseEventUtils.getStartEndUidPair(255, 1);
            Filter filter = HBaseEventUtils.getRowKeyFilter(sortedEvents, dates, uidPair.getFirst(), uidPair.getSecond());
            DirectScanner scanner = new DirectScanner(pair.getFirst(), pair.getSecond(), tableName, filter, false, false);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (scanner.next(results));

            assertEquals(21, results.size());

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void testFilterUid4() {
        System.out.println("Test filter uid. Bucket: 128, 129");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);

            Pair<byte[], byte[]> pair = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), sortedEvents, 128, 2);

            Pair<Long, Long> uidPair = HBaseEventUtils.getStartEndUidPair(128, 2);
            Filter filter = HBaseEventUtils.getRowKeyFilter(sortedEvents, dates, uidPair.getFirst(), uidPair.getSecond());
            DirectScanner scanner = new DirectScanner(pair.getFirst(), pair.getSecond(), tableName, filter, false, false);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (scanner.next(results));
            assertEquals(84, results.size());

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void testFilterEventAndUid1() {
        System.out.println("Test skip event visit.a1.b1.c1. and filter uid(Bucket: 0)");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);

            List<String> testEventSet1 = new ArrayList<String>(sortedEvents);
            testEventSet1.remove(0);

            Pair<Long, Long> uidPair = HBaseEventUtils.getStartEndUidPair(0, 1);
            Pair<byte[], byte[]> pair = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), testEventSet1, 0, 1);

            Filter filter = HBaseEventUtils.getRowKeyFilter(testEventSet1, dates, uidPair.getFirst(), uidPair.getSecond());
            DirectScanner scanner = new DirectScanner(pair.getFirst(), pair.getSecond(), tableName, filter, false, false);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (scanner.next(results));
            assertEquals(36, results.size());

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void testFilterEventAndUid2() {
        System.out.println("Test skip event visit.a1.b1. and visit.a2. and filter uid(Bucket: 80, 81)");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);

            List<String> testEventSet1 = new ArrayList<String>(sortedEvents);
            testEventSet1.remove(1);
            testEventSet1.remove(3);
            Pair<Long, Long> uidPair = HBaseEventUtils.getStartEndUidPair(80, 2);
            Pair<byte[], byte[]> pair = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), testEventSet1, 80, 2);

            Filter filter = HBaseEventUtils.getRowKeyFilter(testEventSet1, dates, uidPair.getFirst(), uidPair.getSecond());
            DirectScanner scanner = new DirectScanner(pair.getFirst(), pair.getSecond(), tableName, filter, false, false);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (scanner.next(results));

            assertEquals(60, results.size());

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void testFilterDateAndEventAndUid() {
        System.out.println("Test skip event visit.a1.b1. and visit.a1.b2., skip date 20130102, filter uid(Bucket: 80, 81)");
        try {
            List<String> sortedEvents = HBaseEventUtils.sortEventList(events);

            List<String> testEventSet1 = new ArrayList<String>(sortedEvents);
            testEventSet1.remove(1);
            testEventSet1.remove(2);

            List<String> testDates1 = new ArrayList<String>(dates);
            testDates1.remove(1);


            Pair<Long, Long> uidPair = HBaseEventUtils.getStartEndUidPair(80, 2);
            Pair<byte[], byte[]> pair = HBaseEventUtils.getStartEndRowKey(String.valueOf(startDate), String.valueOf(endDate), testEventSet1, 80, 2);

            Filter filter = HBaseEventUtils.getRowKeyFilter(testEventSet1, testDates1, uidPair.getFirst(), uidPair.getSecond());
            DirectScanner scanner = new DirectScanner(pair.getFirst(), pair.getSecond(), tableName, filter, false, false);
            List<KeyValue> results = new ArrayList<KeyValue>();
            while (scanner.next(results));

            assertEquals(40, results.size());

        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }


    private static void dropTable() throws IOException {
        System.out.println("Drop hbase test table, before testing...");
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

    private static void flushTable() throws IOException, InterruptedException {
        System.out.println("Begin to flush...");
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        hBaseAdmin.flush(tableName);

    }

    private static void initHBaseFilterdata() throws IOException {
        System.out.println("Begin to insert data to hbase...");
        long startDate = 20130101;
        long endDate = 20130103;

        Map<Integer, Set<Long>> bucketMap = new HashMap<Integer, Set<Long>>();
        for (long i=0; i<totalUidNum; i++) {
            long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(i);
            int bucketNum = (int)(md5Uid >> 32);
            Set<Long> valSet = bucketMap.get(bucketNum);
            if (valSet == null) {
                valSet = new HashSet<Long>();
            }
            valSet.add(i);
            bucketMap.put(bucketNum, valSet);
        }

        for (Map.Entry<Integer, Set<Long>> entry : bucketMap.entrySet()) {
            int bucketNum = entry.getKey();
            Set<Long> valSet = entry.getValue();
            System.out.println("Bucket: " + bucketNum + "\tSize: " + valSet.size());
            for (Long i : valSet) {
                System.out.print(i + "\t");
            }
            System.out.println();

        }

        HTable hTable = new HTable(conf, tableName);
        for (long date=startDate; date<=endDate; date++) {
            for (int i=0; i<totalUidNum; i++) {
                long md5Uid = UidMappingUtil.getInstance().decorateWithMD5(i);
                for (String event : events) {
                    byte[] rowKey = UidMappingUtil.getInstance().getRowKeyV2(String.valueOf(date), event, md5Uid);
                    Put put = new Put(rowKey);
                    put.setWriteToWAL(false);
                    put.add("val".getBytes(), "val".getBytes(), System.currentTimeMillis(), Bytes.toBytes((long) i));
                    hTable.put(put);
                }
            }
        }
        IOUtils.closeStream(hTable);
    }


}
