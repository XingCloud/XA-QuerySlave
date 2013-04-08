package com.xingcloud.hbase.filter;

/**
 * @file UidRangeFilter.java
 * @Description:
 * @author Snake    wangyufei@xingcloud.com
 * @date 2012-5-2 10:53:38
 * @Copyright 2012 Xingcloud Inc. All rights reserved.
 * @version V1.0
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import com.xingcloud.hbase.util.HBaseEventUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;


public class UidRangeFilter extends FilterBase {
  private static Log LOG = LogFactory.getLog(UidRangeFilter.class);

  private final int UID_BYTES_LEN = 5;

  private byte[] startUidOfBytes5;
  private byte[] endUidOfBytes5;

  private Set<String> validEventSet = new HashSet<String>();
  private Deque<String> eventQueue = new LinkedList<String>();

  private byte[] uid = new byte[UID_BYTES_LEN];
  private boolean filterOutRow = false;
  private boolean inValidEvent = false;

  private long warningCounter = 0;

  private String theFirstEvent;

  public UidRangeFilter() {
    super();
  }

  public UidRangeFilter(long startUid, long endUid, Deque<String> eventQueue) {
    byte[] sub = Bytes.toBytes(startUid);
    byte[] eub = Bytes.toBytes(endUid);

    this.startUidOfBytes5 = Arrays.copyOfRange(sub, 3, sub.length);
    this.endUidOfBytes5 = Arrays.copyOfRange(eub, 3, eub.length);
    this.eventQueue = eventQueue;
    this.validEventSet = new HashSet<String>(eventQueue);

    /*Next hint event begin with second*/
    this.theFirstEvent = this.eventQueue.pollFirst();
  }

  @Override
  public void reset() {
    this.filterOutRow = false;
    this.inValidEvent = false;
  }


  @Override
  public ReturnCode filterKeyValue(KeyValue kv) {
    if (this.filterOutRow) {
      if (this.eventQueue.size() == 0) {
        byte[] row = kv.getRow();
        String event = HBaseEventUtils.getEventFromDEURowKey(row);
        if (warningCounter < 10) {
          LOG.warn("------ Event queue is 0. Current event: " + event + " ------");
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

    String event = HBaseEventUtils.getEventFromDEURowKey(rowKeyByteArray);
    uid = HBaseEventUtils.getUidOf5BytesFromDEURowKey(rowKeyByteArray);

    if (!validEventSet.contains(event)) {
      this.inValidEvent = true;
      this.filterOutRow = true;
      return this.filterOutRow;
    }

    if (Bytes.compareTo(uid, startUidOfBytes5) < 0 || Bytes.compareTo(uid, endUidOfBytes5) >= 0) {
      this.filterOutRow = true;
    }
    return this.filterOutRow;
  }

  @Override
  public KeyValue getNextKeyHint(KeyValue kv) {
    byte[] rk = kv.getRow();
    byte[] date = Arrays.copyOfRange(rk, 0, 8);
    if (inValidEvent) {
            /*This event is not contained in query event set*/
            /*Because we just take at most 2000 different events, so this may happen. This means in hbase but can't query from mongodb*/
      String nextEvent = eventQueue.pollFirst();
      byte[] newRowKey = HBaseEventUtils.getRowKey(date, nextEvent, startUidOfBytes5);

      KeyValue newKV = new KeyValue(newRowKey, kv.getFamily(), kv.getQualifier());
//            logger.info("Using next hint(0): current row: " + new String(kv.getRow()) + " to " + new String(newRowKey));
      return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
              .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
              .getFamilyLength(), null, 0, 0);
    }

    if (Bytes.compareTo(uid, startUidOfBytes5) < 0) {
      byte[] newRowKey = HBaseEventUtils.changeRowKeyByUid(rk, startUidOfBytes5);
      KeyValue newKV = new KeyValue(newRowKey, kv.getFamily(), kv.getQualifier());
//            logger.info("Using next hint(1): current row: " + new String(kv.getRow()) + " to " + new String(newKV.getRow()) + " current uid: " + new String(uid)
//                + " start uid: " + new String(startUid));
      return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
              .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
              .getFamilyLength(), null, 0, 0);
    } else {
      String currentEvent = HBaseEventUtils.getEventFromDEURowKey(rk);
      while ((eventQueue.peekFirst() + String.valueOf((char) 255)).compareTo(currentEvent + String.valueOf(
              (char) 255)) <= 0) {
                /*The first event in this region is not the first one in event queue, ex. buy.a, buy.b, buy.c, buy.d, the first one in this region is buy.c*/
        eventQueue.pollFirst();
      }
      byte[] newRowKey = HBaseEventUtils.getRowKey(date, eventQueue.pollFirst(), startUidOfBytes5);
//            logger.info("Using next hint(2): curent row: " + new String(kv.getRow()) + "to " + newRowKey);
      KeyValue newKV = new KeyValue(newRowKey, kv.getFamily(), kv.getQualifier());
      return KeyValue.createFirstOnRow(newKV.getBuffer(), newKV.getRowOffset(), newKV
              .getRowLength(), newKV.getBuffer(), newKV.getFamilyOffset(), newKV
              .getFamilyLength(), null, 0, 0);
    }

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.startUidOfBytes5 = Bytes.readByteArray(in);
    this.endUidOfBytes5 = Bytes.readByteArray(in);
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      String event = new String(Bytes.readByteArray(in));
      eventQueue.add(event);
      LOG.info("read event:" + event);
    }

    //do the same thing in constructor with args
    this.validEventSet = new HashSet<String>(eventQueue);
        /*Next hint event begin with second*/
    this.eventQueue.pollFirst();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Bytes.writeByteArray(out, startUidOfBytes5);
    Bytes.writeByteArray(out, endUidOfBytes5);
    out.writeInt(validEventSet.size());

    //eventQueue has been polled first
    Bytes.writeByteArray(out, Bytes.toBytes(theFirstEvent));
    for (String event : eventQueue) {
      Bytes.writeByteArray(out, Bytes.toBytes(event));
    }
  }

}