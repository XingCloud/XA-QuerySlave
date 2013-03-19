package org.apache.hadoop.hbase.regionserver;

import com.xingcloud.hbase.meta.HBaseMeta;
import com.xingcloud.hbase.util.FileManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Scan;
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
public class HFileScanner implements DataScanner {
    private static Logger LOG = LoggerFactory.getLogger(RegionScanner.class);

    private final String familyName = "val";
    private final int batch = 1000;

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

    private KeyValueHeap storeHeap;
    List<KeyValue> results = new ArrayList<KeyValue>();
    private int isScan;
    private final byte [] stopRow;


    public HFileScanner(HRegionInfo hRegionInfo, Scan scan) throws IOException {
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
        String[] lastField =  fields[fields.length-1].split("\\.");
        return lastField[1];
    }


    private void initKVScanners(Scan scan) {
        LOG.info("Init KV scanner...");
        List<KeyValueScanner> kvScanners = new ArrayList<KeyValueScanner>();
        try {
            long timeToPurgeDeletes =
                    Math.max(conf.getLong("hbase.hstore.time.to.purge.deletes", 0), 0);
            this.scanInfo = new Store.ScanInfo(this.family.getName(), this.family.getMinVersions(),
                    this.family.getMaxVersions(), ttl, this.family.getKeepDeletedCells(),
                    timeToPurgeDeletes, this.comparator);
            long oldestUnexpiredTS = EnvironmentEdgeManager.currentTimeMillis() - ttl;

            for (Map.Entry<byte[], NavigableSet<byte[]>> entry :
                    scan.getFamilyMap().entrySet()) {
                this.matcher = new ScanQueryMatcher(scan, this.scanInfo, entry.getValue(), StoreScanner.ScanType.USER_SCAN,
                        Long.MAX_VALUE, HConstants.LATEST_TIMESTAMP, oldestUnexpiredTS);
                List<StoreFileScanner> sfScanners = StoreFileScanner.getScannersForStoreFiles(storeFiles, false, false);
                this.scanners = new ArrayList<KeyValueScanner>(sfScanners.size());
                this.scanners.addAll(sfScanners);

                StoreScanner storeScanner = new StoreScanner(scan, scanInfo, StoreScanner.ScanType.USER_SCAN, entry.getValue(), scanners);
                kvScanners.add(storeScanner);
                /* Only have one column family */
                break;
            }
            this.storeHeap = new KeyValueHeap(kvScanners, this.comparator);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("initKVScanners got exception! MSG: " + e.getMessage());
        }

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
        return returnResult;
    }

    private boolean nextInternal(int limit) throws IOException {
        while (true) {
            byte [] currentRow = peekRow();
            if (isStopRow(currentRow)) {
                return false;
            } else {
                byte[] nextRow;
                do {
                    this.storeHeap.next(results, limit - results.size());
                    if (limit > 0 && results.size() == limit) {
                        return true;
                    }
                } while (Bytes.equals(currentRow, nextRow = peekRow()));

                final boolean stopRow = isStopRow(nextRow);

                if (results.isEmpty()) {
                    nextRow(currentRow);
                    if (!stopRow) continue;
                }
                return !stopRow;
            }
        }
    }

    private void nextRow(byte [] currentRow) throws IOException {
        while (Bytes.equals(currentRow, peekRow())) {
            this.storeHeap.next(MOCKED_LIST);
        }
        results.clear();
    }

    private byte[] peekRow() {
        KeyValue kv = this.storeHeap.peek();
        return kv == null ? null : kv.getRow();
    }

    private boolean isStopRow(byte [] currentRow) {
        return currentRow == null ||
                (stopRow != null &&
                        comparator.compareRows(stopRow, 0, stopRow.length,
                                currentRow, 0, currentRow.length) <= isScan);
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
