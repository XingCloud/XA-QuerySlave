package org.apache.drill.exec.ref.rse;
import com.xingcloud.hbase.util.HBaseEventUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.UnbackedRecord;
import org.apache.drill.exec.ref.exceptions.RecordException;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.drill.exec.ref.rse.RecordReader;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;
import org.apache.drill.exec.ref.values.SimpleMapValue;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.TableScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.xerces.impl.dv.util.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.drill.exec.ref.ReferenceInterpreter;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-11
 * Time: 下午2:06
 * To change this template use File | Settings | File Templates.
 */
public class HBaseRecordReader implements RecordReader {
    private static Logger LOG = LoggerFactory.getLogger(HBaseRecordReader.class);

    private long totalRecords = 0;
    private List<TableScanner> scanners = new ArrayList<TableScanner>();
    private int currentScannerIndex = 0;
    private List<KeyValue> curRes = new ArrayList<KeyValue>();
    private int valIndex = -1;
    private boolean hasMore;

    private ROP parent;
    private SchemaPath rootPath;
    private UnbackedRecord record = new UnbackedRecord();
  
  public  HBaseRecordReader(String startKey, String endKey, Filter filter, ROP parent, SchemaPath rootPath){
    this.parent = parent;
    this.rootPath = rootPath;
    TableScanner tableScanner = new TableScanner(Base64.decode(startKey), Base64.decode(endKey), getTableName(rootPath), filter, false, false);   
    scanners.add(tableScanner);
  }
    private class NodeIter implements RecordIterator {


        @Override
        public RecordPointer getRecordPointer() {
            return record;
        }

        @Override
        public NextOutcome next() {
            try {
                NextOutcome outcome = next(scanners.get(currentScannerIndex));
                while (outcome == NextOutcome.NONE_LEFT) {
                    currentScannerIndex++;
                    if (currentScannerIndex > scanners.size()-1) {
                        return NextOutcome.NONE_LEFT;
                    }
                    outcome = next(scanners.get(currentScannerIndex));
                }
                return outcome;
            } catch (IOException e) {
                throw new RecordException("Failure while reading record", null, e);
            }
        }

        @Override
        public ROP getParent() {
            return parent;
        }

        private NextOutcome next(TableScanner scanner) throws IOException {
            //System.out.println(totalRecords++);
            if (valIndex == -1) {
                if (scanner == null) {
                    return RecordIterator.NextOutcome.NONE_LEFT;
                }
                hasMore = scanner.next(curRes);
                valIndex = 0;
            }

            if (valIndex > curRes.size()-1) {
                while (hasMore) {
                        /* Get result list from the same scanner and skip curRes with no element */
                    curRes.clear();
                    hasMore = scanner.next(curRes);
                    valIndex = 0;
                    if (curRes.size() != 0) {
                        KeyValue kv = curRes.get(valIndex++);
                      try{
                        record.setClearAndSetRoot(rootPath, convert(kv));
                      }catch (Exception e){
                        e.printStackTrace();
                      }
                        return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
                    }
                }
                return NextOutcome.NONE_LEFT;
            }
            KeyValue kv = curRes.get(valIndex++);
            record.setClearAndSetRoot(rootPath, convert(kv));
            return NextOutcome.INCREMENTED_SCHEMA_CHANGED;
        }

    }



    @Override
    public RecordIterator getIterator() {
        return new NodeIter();
    }

    @Override
    public void setup() {
    }

    @Override
    public void cleanup() {
        for (TableScanner scanner : scanners) {
            try {
                scanner.close();
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("Error while closing Scanner " + scanner, e);
            }
        }
    }



    public DataValue convert(KeyValue kv) {

        SimpleMapValue map = new SimpleMapValue();
        byte[] rk = kv.getRow();

        /* Set uid */
        long uid = HBaseEventUtils.getUidOfLongFromDEURowKey(rk);
        int innerUid = HBaseEventUtils.getInnerUidFromSamplingUid(uid);
        DataValue uidDV = new ScalarValues.IntegerScalar(innerUid);
        map.setByName("uid", uidDV);
        /* Set event value */
        long eventVal = Bytes.toLong(kv.getValue());
        DataValue valDV = new ScalarValues.LongScalar(eventVal);
        map.setByName("value", valDV);
        /* Set event name */
        String event = HBaseEventUtils.getEventFromDEURowKey(rk);
        String[] fields = event.split("\\.");
        int i = 0;
        for (;i<fields.length;i++) {
            String name = fields[i];
            DataValue nameDV = new ScalarValues.StringScalar(name);
            map.setByName("l"+i, nameDV);
        }
        for (; i<5; i++) {
            DataValue nameDV = new ScalarValues.StringScalar("*");
            map.setByName("l"+i, nameDV);
        }
        /* Set timestamp */
        long ts = kv.getTimestamp();
        DataValue tsDV = new ScalarValues.LongScalar(ts);
        map.setByName("ts", tsDV);

        map.setByName("row", new ScalarValues.StringScalar(Base64.encode(rk)));
        //LOG.info(map.toString());
        //System.out.println(innerUid+"\t"+eventVal+"\t"+ts);
        return map;
    }

    public String calDay(String date, int dis) throws ParseException {
        try {
            TimeZone TZ = TimeZone.getTimeZone("GMT+8");
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            Date temp = new Date(getTimestamp(date));

            java.util.Calendar ca = Calendar.getInstance(TZ);
            ca.setTime(temp);
            ca.add(Calendar.DAY_OF_MONTH, dis);
            return df.format(ca.getTime());
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("CalDay got exception! " + date + " " + dis);
            throw new ParseException(date + " " + dis, 0);
        }
    }

    public long getTimestamp(String date) {
        String dateString = date + " 00:00:00";
        SimpleDateFormat tdf = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
        Date nowDate = null;
        try {
            nowDate = tdf.parse(dateString);
        } catch (ParseException e) {
            LOG.error("DateManager.daydis catch Exception with params is "
                    + date, e);
        }
        if (nowDate != null) {
            return nowDate.getTime();
        } else {
            return -1;
        }
    }

    public int compareDate(String DATE1, String DATE2) throws ParseException{
        try {
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            Date dt1 = df.parse(DATE1);
            Date dt2 = df.parse(DATE2);
            if (dt1.getTime() > dt2.getTime()) {
                return 1;
            } else if (dt1.getTime() < dt2.getTime()) {
                return -1;
            } else {
                return 0;
            }
        } catch (Exception e) {
            LOG.error("Invalid date format! Date1: " + DATE1 + "\tDate2: " + DATE2, e);
            e.printStackTrace();
            throw new ParseException(DATE1 + "\t" + DATE2, 0);
        }

    }

    public String getNextEvent(String eventFilter) {
        StringBuilder endEvent = new StringBuilder(eventFilter);
        endEvent.setCharAt(eventFilter.length() - 1, (char) (endEvent.charAt(eventFilter.length() - 1) + 1));
        return endEvent.toString();
    }

  public String getTableName(SchemaPath rootPath) {
    return rootPath.getPath().toString().replace("xadrill", "-");
  }
  
    public static void main(String[] args) throws Exception{
        //HBaseRecordReader hBaseRecordReader = new HBaseRecordReader();
    }
}
