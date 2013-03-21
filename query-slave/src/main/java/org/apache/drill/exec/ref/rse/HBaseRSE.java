package org.apache.drill.exec.ref.rse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StorageEngineConfigBase;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.drill.exec.ref.rse.RSEBase;
import org.apache.drill.exec.ref.rse.RecordReader;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 3/11/13
 * Time: 6:01 AM
 * To change this template use File | Settings | File Templates.
 */
public class HBaseRSE  extends RSEBase {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HBaseRSE.class);

    public HBaseRSE(HBaseRSEConfig engineConfig, DrillConfig config){

    }

    @JsonTypeName("hbase")
    public static class HBaseRSEConfig extends StorageEngineConfigBase {
        @JsonCreator
        public HBaseRSEConfig(@JsonProperty("name") String name) {
            super(name);
        }
    }
    public static class HBaseInputConfig implements ReadEntry{
        public String startDate;
        public String endDate;
        public String l0;
        public String l1;
        public String l2;
        public String l3;
        public String l4;
        @JsonIgnore
        public SchemaPath rootPath;
    }

    @Override
    public Collection<ReadEntry> getReadEntries(Scan scan) throws IOException {
        HBaseInputConfig hBaseInputConfig = scan.getSelection().getWith(HBaseInputConfig.class);
        hBaseInputConfig.rootPath = scan.getOutputReference();
        return Collections.singleton((ReadEntry)hBaseInputConfig);
    }

    @Override
    public RecordReader getReader(ReadEntry readEntry, ROP parentROP) throws IOException {
        HBaseInputConfig e = getReadEntry(HBaseInputConfig.class, readEntry);
        return new HBaseRecordReader(e.startDate, e.endDate, e.l0, e.l1, e.l2, e.l3, e.l4, parentROP, e.rootPath);
    }
}
