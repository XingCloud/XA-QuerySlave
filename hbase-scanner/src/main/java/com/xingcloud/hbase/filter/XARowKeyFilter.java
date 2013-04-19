package com.xingcloud.hbase.filter;

import com.xingcloud.hbase.util.HBaseEventUtils;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-4-12
 * Time: 上午11:51
 * To change this template use File | Settings | File Templates.
 */
public class XARowKeyFilter extends FilterBase {
    private static Log LOG = LogFactory.getLog(XARowKeyFilter.class);

    private byte[] startUidOfBytes5;
    private byte[] endUidOfBytes5;

    private Set<String> validEventSet = null;
    private List<String> events = new ArrayList<String>();
    private int eventIndex = 0;

    private Set<String> validDateSet = null;
    private List<String> dates = new ArrayList<String>();
    private int dateIndex = 0;

    private String date;
    private String event;

    private boolean filterOutRow = false;
    private boolean inValidEvent = false;
    private boolean inValidDate = false;
    private boolean lessThanStartUid = false;
    private boolean greaterThanEndUid = false;

    private long warningCounter = 0;

    public XARowKeyFilter() {
        super();
    }

    public XARowKeyFilter(long startUid, long endUid, List<String> events, List<String> dates) {
        byte[] sub = Bytes.toBytes(startUid);
        byte[] eub = Bytes.toBytes(endUid);

        this.startUidOfBytes5 = Arrays.copyOfRange(sub, 3, sub.length);
        this.endUidOfBytes5 = Arrays.copyOfRange(eub, 3, eub.length);
        this.events = events;
        this.validEventSet = new HashSet<String>(events);
        this.dates = dates;
        this.validDateSet = new HashSet<String>(this.dates);
    }

    @Override
    public void reset() {
        this.filterOutRow = false;
        this.inValidEvent = false;
        this.inValidDate = false;
    }


    @Override
    public ReturnCode filterKeyValue(KeyValue kv) {
        if (this.filterOutRow) {
            if (this.eventIndex==this.events.size() && this.dateIndex==this.dates.size()) {
                byte[] row = kv.getRow();
                String event = HBaseEventUtils.getEventFromDEURowKey(row);
                if (warningCounter < 10) {
                    LOG.warn("------ Event queue and date queue is 0. Current event: " + event + " ------");
                }
                warningCounter++;
                return ReturnCode.NEXT_ROW;
            }
            return ReturnCode.SEEK_NEXT_USING_HINT;
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterRowKey(byte[] data, int offset, int length) {
        byte[] rowKeyByteArray = Arrays.copyOfRange(data, offset, offset + length);

        date = Bytes.toString(Arrays.copyOfRange(rowKeyByteArray, 0, 8));
        if (!validDateSet.contains(date)) {
            this.inValidDate = true;
            this.filterOutRow = true;
            return this.filterOutRow;
        }

        event = HBaseEventUtils.getEventFromDEURowKey(rowKeyByteArray);

        if (!validEventSet.contains(event)) {
            this.inValidEvent = true;
            this.filterOutRow = true;
            return this.filterOutRow;
        }

        byte[] uid = HBaseEventUtils.getUidOf5BytesFromDEURowKey(rowKeyByteArray);

        if (Bytes.compareTo(uid, startUidOfBytes5) < 0) {
            this.lessThanStartUid = true;
            this.filterOutRow = true;
        } else if (Bytes.compareTo(uid, endUidOfBytes5) >= 0) {
            this.greaterThanEndUid = true;
            this.filterOutRow = true;
        }
        return this.filterOutRow;
    }

    @Override
    public KeyValue getNextKeyHint(KeyValue kv) {
        byte[] rk = kv.getRow();
        resetIndex();

        if (inValidDate) {
            /* Date set doesn't contain this date */
            while (date.compareTo(dates.get(dateIndex))>=0) {
                dateIndex++;
            }
            String nextEvent = events.get(eventIndex);
            byte[] nextRowKey = HBaseEventUtils.getRowKey(Bytes.toBytes(dates.get(dateIndex)), nextEvent, startUidOfBytes5);
            KeyValue newKV = new KeyValue(nextRowKey, kv.getFamily(), kv.getQualifier());
            return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
                    .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
                    .getFamilyLength(), null, 0, 0);
        }

        if (inValidEvent) {
            /* Event set doesn't contain this event */

            /* Move to current date */
            while (!date.equals(dates.get(dateIndex))) {
                dateIndex++;
            }
            /* Adjust event index to next hint event */
            while ((event + String.valueOf((char) 255)).compareTo((events.get(eventIndex) + String.valueOf((char) 255)))>=0) {
                eventIndex++;
                if (eventIndex == events.size()) {
                    break;
                }
            }

            if (eventIndex == events.size()) {
                /* Move to next date */
                while (date.compareTo(dates.get(dateIndex))>=0) {
                    dateIndex++;
                }
                eventIndex = 0;
            }


            String nextEvent = events.get(eventIndex);
            byte[] newRowKey = HBaseEventUtils.getRowKey(Bytes.toBytes(dates.get(dateIndex)), nextEvent, startUidOfBytes5);

            KeyValue newKV = new KeyValue(newRowKey, kv.getFamily(), kv.getQualifier());
            return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
                    .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
                    .getFamilyLength(), null, 0, 0);
        }

        if (lessThanStartUid) {
            byte[] newRowKey = HBaseEventUtils.changeRowKeyByUid(rk, startUidOfBytes5);
            KeyValue newKV = new KeyValue(newRowKey, kv.getFamily(), kv.getQualifier());

            return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
                    .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
                    .getFamilyLength(), null, 0, 0);
        } else {
           /* Adjust event index to point current index */
            while ((event + String.valueOf((char) 255)).compareTo((events.get(eventIndex) + String.valueOf((char) 255)))>=0) {
                eventIndex++;
                if (eventIndex == events.size()) {
                    break;
                }
            }

            if (eventIndex == events.size()) {
                /* Move to next date */
                while (date.compareTo(dates.get(dateIndex))>=0) {
                    dateIndex++;
                }
                eventIndex = 0;
            } else {
                /* Move to current date */
                while (!date.equals(dates.get(dateIndex))) {
                    dateIndex++;
                }
            }


            byte[] newRowKey = HBaseEventUtils.getRowKey(Bytes.toBytes(dates.get(dateIndex)), events.get(eventIndex), startUidOfBytes5);

            KeyValue newKV = new KeyValue(newRowKey, kv.getFamily(), kv.getQualifier());
            return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
                    .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
                    .getFamilyLength(), null, 0, 0);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        LOG.info("Read fields of XARowKeyFilter...");
        this.startUidOfBytes5 = Bytes.readByteArray(in);
        this.endUidOfBytes5 = Bytes.readByteArray(in);
        int size = in.readInt();
        LOG.info("Event size: " + size);
        List<String> events = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            String event = new String(Bytes.readByteArray(in));
            LOG.info("Read event " + event);
            events.add(event);
        }
        this.validEventSet = new HashSet<String>(events);

        size = in.readInt();
        LOG.info("Date size: " + size);
        dates = new ArrayList<String>();
        for (int i = 0; i < size; i++) {
            String date = Bytes.toString(Bytes.readByteArray(in));
            LOG.info("Read date " + date);
            dates.add(date);
        }
        this.validDateSet = new HashSet<String>(dates);
        this.filterOutRow = false;
        this.inValidEvent = false;
        this.inValidDate = false;
        this.lessThanStartUid = false;
        this.greaterThanEndUid = false;

        this.eventIndex = 0;
        this.dateIndex = 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Bytes.writeByteArray(out, startUidOfBytes5);
        Bytes.writeByteArray(out, endUidOfBytes5);

        out.writeInt(validEventSet.size());
        for (String event : events) {
            LOG.info("Write event: " + event);
            Bytes.writeByteArray(out, Bytes.toBytes(event));
        }

        out.writeInt(validDateSet.size());
        for (String date : dates) {
            LOG.info("Write date: " + date);
            Bytes.writeByteArray(out, Bytes.toBytes(date));
        }
    }

    private void resetIndex() {
        dateIndex = 0;
        eventIndex = 0;
    }
}
