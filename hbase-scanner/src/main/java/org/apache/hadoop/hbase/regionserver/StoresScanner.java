package org.apache.hadoop.hbase.regionserver;

import com.xingcloud.hbase.manager.HBaseResourceManager;
import com.xingcloud.hbase.meta.HBaseMeta;
import com.xingcloud.hbase.util.FileManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-3-1
 * Time: 下午3:40
 * To change this template use File | Settings | File Templates.
 */
public class StoresScanner implements XAScanner {
  private static Logger LOG = LoggerFactory.getLogger(RegionScanner.class);

  private final String familyName = "val";
  private final int batch = 1000;//todo

  private Scan scan;

  private FileSystem fs;
  private Configuration conf;
  private CacheConfig cacheConf;

  private String ip;

  private StoreFile.BloomType bloomType = StoreFile.BloomType.NONE;
  private DataBlockEncoding dataBlockEncoding;
  private Compression.Algorithm compression;
  private HFileDataBlockEncoder dataBlockEncoder;
  private long ttl;

  private AtomicLong numSeeks = new AtomicLong();
  private AtomicLong numKV = new AtomicLong();
  private AtomicLong totalBytes = new AtomicLong();

  private HRegionInfo hRegionInfo;
  private KeyValue.KVComparator comparator;
  private HColumnDescriptor family;
  private List<StoreFile> storeFiles = new ArrayList<StoreFile>();
  private List<KeyValueScanner> scanners;
  private ScanQueryMatcher matcher;
  private Store.ScanInfo scanInfo;

  private Filter filter;

  private KeyValueHeap storeHeap;
  List<KeyValue> results = new ArrayList<KeyValue>();
  private int isScan;
  private final byte[] stopRow;

  private final KeyValue KV_LIMIT = new KeyValue();


  Map<String, KeyValueScanner> storeScanners =  new HashMap<String, KeyValueScanner>();

  public StoresScanner(HRegionInfo hRegionInfo, Scan scan) throws IOException {
    InetAddress addr = InetAddress.getLocalHost();
    this.ip = addr.getHostAddress();

    this.hRegionInfo = hRegionInfo;
    this.comparator = hRegionInfo.getComparator();

    this.scan = scan;
    this.isScan = scan.isGetScan() ? -1 : 0;
    if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)) {
      this.stopRow = null;
    } else {
      this.stopRow = scan.getStopRow();
    }

    this.conf = HBaseConfiguration.create();
    this.cacheConf = new CacheConfig(this.conf);
    this.fs = FileSystem.get(conf);
    this.filter = scan.getFilter();


    initColFamily(hRegionInfo, familyName);
    initStoreFiles(hRegionInfo);
    initKVScanners(scan);
  }

  private void initColFamily(HRegionInfo hRegionInfo, String familyName) {
    LOG.info("Init column family...");
    HColumnDescriptor[] families = hRegionInfo.getTableDesc().getColumnFamilies();
    for (HColumnDescriptor eachFamily : families) {
      if (eachFamily.getNameAsString().equals(familyName)) {
        this.family = eachFamily;
        LOG.info("Column family info: " + this.family.toString());
        this.dataBlockEncoder = new HFileDataBlockEncoderImpl(this.family.getDataBlockEncodingOnDisk(),
                this.family.getDataBlockEncoding());
        this.compression = this.family.getCompactionCompression();

        this.ttl = this.family.getTimeToLive();
        if (ttl == HConstants.FOREVER) {
          ttl = Long.MAX_VALUE;
        } else if (ttl == -1) {
          ttl = Long.MAX_VALUE;
        } else {
          this.ttl *= 1000;
        }

        break;
      }
    }
  }

  private void initStoreFiles(HRegionInfo hRegionInfo) throws IOException {
    LOG.info("Init store files...");
    String tableDir = HBaseMeta.getTablePath(hRegionInfo.getTableNameAsString(), conf);
    String regionName = getRegionName(hRegionInfo);
    String regionPath = tableDir + regionName + "/val/";
    /* Get store file path list */
    List<Path> storeFilePaths = FileManager.listDirPath(regionPath);
    Collections.sort(storeFilePaths);
    /* Init each store file */
    for (Path path : storeFilePaths) {
      StoreFile sf = openStoreFile(path);
      storeFiles.add(sf);
      LOG.info("Add store file " + path.toString());
    }

  }

  private String getRegionName(HRegionInfo hRegionInfo) {
    String regionName = hRegionInfo.getRegionNameAsString();
    String[] fields = regionName.split(",");
    String[] lastField = fields[fields.length - 1].split("\\.");
    return lastField[1];
  }


  private void initKVScanners(Scan scan) {
    LOG.info("Init KV scanner...");
    try {
      long timeToPurgeDeletes =
              Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
      this.scanInfo = new Store.ScanInfo(this.family.getName(), this.family.getMinVersions(),
              this.family.getMaxVersions(), ttl, this.family.getKeepDeletedCells(),
              timeToPurgeDeletes, this.comparator);
      long oldestUnexpiredTS = EnvironmentEdgeManager.currentTimeMillis() - ttl;

      for (Map.Entry<byte[], NavigableSet<byte[]>> entry :
              scan.getFamilyMap().entrySet()) {
        this.matcher = new ScanQueryMatcher(scan, this.scanInfo, entry.getValue(), ScanType.USER_SCAN,
                Long.MAX_VALUE, HConstants.LATEST_TIMESTAMP, oldestUnexpiredTS);
        List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(storeFiles, false, false);
        this.scanners = new ArrayList<KeyValueScanner>(sfScanners.size());
        this.scanners.addAll(sfScanners);

        StoreScanner storeScanner = new StoreScanner(scan, scanInfo, ScanType.USER_SCAN, entry.getValue(), scanners);
        storeScanners.put(Bytes.toString(entry.getKey()), storeScanner);
        break; /* Only have one column family */
      }
      this.storeHeap = new KeyValueHeap(new ArrayList<KeyValueScanner>(storeScanners.values()), this.comparator);
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("initKVScanners got exception! MSG: " + e.getMessage());
    }

  }
  
  public void updateScanner(byte[] family, KeyValue theNext) throws IOException {
    //((StoreScanner)storeScanners.get(Bytes.toString(family))).updateReaders();
    initStoreFiles(hRegionInfo);
    initKVScanners(scan);
    storeScanners.get(Bytes.toString(family)).seek(theNext);
  }
  
  public boolean next(List<KeyValue> outResults) throws IOException {
    boolean hasMore = next(outResults, batch);
    numKV.addAndGet(outResults.size());
    return hasMore;
  }
  
  public boolean next(List<KeyValue> outResults, int limit) throws IOException {
    results.clear();
    boolean returnResult = nextInternal(limit);
    outResults.addAll(results);
    resetFilters();
    return !isFilterDone() && returnResult;
  }

  private boolean nextInternal(int limit) throws IOException {
    while (true) {

      // Let's see what we have in the storeHeap.
      KeyValue current = this.storeHeap.peek();

      byte[] currentRow = null;
      int offset = 0;
      short length = 0;
      if (current != null) {
        currentRow = current.getBuffer();
        offset = current.getRowOffset();
        length = current.getRowLength();
      }

      // First, check if we are at a stop row. If so, there are no more results.
      boolean stopRow = isStopRow(currentRow, offset, length);
      if (stopRow) {
        if (filter != null && filter.hasFilterRow()) {
          filter.filterRow(results);
        }
        if (filter != null && filter.filterRow()) {
          results.clear();
        }
        return false;
      }
      // Check if rowkey filter wants to exclude this row. If so, loop to next.
      // Techically, if we hit limits before on this row, we don't need this call.

      if (filterRowKey(currentRow, offset, length)) {
        boolean moreRows = nextRow(currentRow, offset, length);
        if (!moreRows) return false;
        continue;
      }

      // Ok, we are good, let's try to get some results from the main heap.
      KeyValue nextKv = populateResult(this.storeHeap, limit, currentRow, offset, length);
      if (nextKv == KV_LIMIT) {
        if (this.filter != null && filter.hasFilterRow()) {
          throw new IncompatibleFilterException(
                  "Filter whose hasFilterRow() returns true is incompatible with scan with limit!");
        }
        return true; // We hit the limit.
      }
      stopRow = nextKv == null || isStopRow(nextKv.getBuffer(), nextKv.getRowOffset(), nextKv.getRowLength());
      // save that the row was empty before filters applied to it.
      final boolean isEmptyRow = results.isEmpty();

      // We have the part of the row necessary for filtering (all of it, usually).
      // First filter with the filterRow(List).
      if (filter != null && filter.hasFilterRow()) {
        filter.filterRow(results);
      }

      if (isEmptyRow || filterRow()) {
        // this seems like a redundant step - we already consumed the row
        // there're no left overs.
        // the reasons for calling this method are:
        // 1. reset the filters.
        // 2. provide a hook to fast forward the row (used by subclasses)
        boolean moreRows = nextRow(currentRow, offset, length);
        if (!moreRows) return false;

        // This row was totally filtered out, if this is NOT the last row,
        // we should continue on. Otherwise, nothing else to do.
        if (!stopRow) continue;
        return false;
      }

      // Finally, we are done with both joinedHeap and storeHeap.
      // Double check to prevent empty rows from appearing in result. It could be
      // the case when SingleValueExcludeFilter is used.
      if (results.isEmpty()) {
        boolean moreRows = nextRow(currentRow, offset, length);
        if (!moreRows) return false;
        if (!stopRow) continue;
      }
      return !stopRow;
    }
  }

  protected boolean nextRow(byte[] currentRow, int offset, short length) throws IOException {
    KeyValue next;
    while ((next = this.storeHeap.peek()) != null && next.matchingRow(currentRow, offset, length)) {
      this.storeHeap.next(MOCKED_LIST);
    }
    results.clear();
    resetFilters();

    return true;
  }

  private boolean filterRow() {
    return filter != null
            && filter.filterRow();
  }

  /**
   * Reset both the filter and the old filter.
   */
  protected void resetFilters() {
    if (filter != null) {
      filter.reset();
    }
  }

  private boolean isStopRow(byte[] currentRow, int offset, short length) {
    return currentRow == null ||
            (stopRow != null &&
                    comparator.compareRows(stopRow, 0, stopRow.length,
                            currentRow, offset, length) <= isScan);
  }

  private boolean filterRowKey(byte[] row, int offset, short length) {
    return filter != null
            && filter.filterRowKey(row, offset, length);
  }

  /**
   * Fetches records with this row into result list, until next row or limit (if not -1).
   *
   * @param heap       KeyValueHeap to fetch data from. It must be positioned on correct row before call.
   * @param limit      Max amount of KVs to place in result list, -1 means no limit.
   * @param currentRow Byte array with key we are fetching.
   * @param offset     offset for currentRow
   * @param length     length for currentRow
   * @return true if limit reached, false otherwise.
   */
  private KeyValue populateResult(KeyValueHeap heap, int limit, byte[] currentRow, int offset,
                                  short length) throws IOException {
    KeyValue nextKv;
    do {
      heap.next(results, limit - results.size());
      if (limit > 0 && results.size() == limit) {
        return KV_LIMIT;
      }
      nextKv = heap.peek();
    } while (nextKv != null && nextKv.matchingRow(currentRow, offset, length));
    return nextKv;
  }

  /*
     * @return True if a filter rules the scanner is over, done.
     */
  public synchronized boolean isFilterDone() {
    return this.filter != null && this.filter.filterAllRemaining();
  }

  public void close() throws IOException {
    for (KeyValueScanner scanner : scanners) {
      scanner.close();
    }
    LOG.info("Total kv number from hfile: " + numKV.toString());
  }

  private StoreFile openStoreFile(Path filePath) {
    LOG.info("Open store file for " + filePath);
    StoreFile sf = null;
    try {
      sf = new StoreFile(fs, filePath, conf, cacheConf, this.family.getBloomFilterType(), dataBlockEncoder);
    } catch (Exception e) {
      e.printStackTrace();
    }
    LOG.info("Store file init successfully!");
    return sf;
  }

  private static final List<KeyValue> MOCKED_LIST = new AbstractList<KeyValue>() {

    @Override
    public void add(int index, KeyValue element) {
      // do nothing
    }

    @Override
    public boolean addAll(int index, Collection<? extends KeyValue> c) {
      return false; // this list is never changed as a result of an update
    }

    @Override
    public KeyValue get(int index) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
      return 0;
    }
  };

}
