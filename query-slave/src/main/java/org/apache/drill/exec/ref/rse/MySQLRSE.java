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
import org.apache.drill.exec.ref.rse.ReferenceStorageEngine;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 3/11/13
 * Time: 6:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class MySQLRSE extends RSEBase {

    public MySQLRSE(MySQLRSEConfig engineConfig, DrillConfig config){

    }

    @JsonTypeName("mysql")
    public static class MySQLRSEConfig extends StorageEngineConfigBase {
        @JsonCreator
        public MySQLRSEConfig(@JsonProperty("name") String name) {
            super(name);
        }
    }

    public static class MySQLInputConfig implements ReferenceStorageEngine.ReadEntry {
        public String sql;
        @JsonIgnore
        public SchemaPath rootPath;
    }

    @Override
    public Collection<ReferenceStorageEngine.ReadEntry> getReadEntries(Scan scan) throws IOException {
        MySQLInputConfig mySQLInputConfig = scan.getSelection().getWith(MySQLInputConfig.class);
        mySQLInputConfig.rootPath = scan.getOutputReference();
        return Collections.singleton((ReferenceStorageEngine.ReadEntry) mySQLInputConfig);
    }

    @Override
    public RecordReader getReader(ReadEntry readEntry, ROP parentROP) throws IOException {
        MySQLInputConfig e = getReadEntry(MySQLInputConfig.class, readEntry);
        return new MysqlRecordReader(e.sql, parentROP, e.rootPath);
        //return  new MysqlRecordReader("select * from register_time where register_time.val>=20130101000000 and register_time.val<20130102000000", parentROP, e.rootPath);
    }
}
