package org.apache.drill.exec.ref.rse;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StorageEngineConfigBase;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.drill.exec.ref.rse.RSEBase;
import org.apache.drill.exec.ref.rse.RecordReader;
import org.apache.drill.exec.ref.rse.ReferenceStorageEngine;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties ;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 3/11/13
 * Time: 6:04 AM
 * To change this template use File | Settings | File Templates.
 */
public class MySQLRSE extends RSEBase {

    static ComboPooledDataSource cpds = null;

    public static Connection getConnection() throws Exception {
        if (cpds == null) {
            System.setProperty("com.mchange.v2.c3p0.cfg.xml",FileUtils.class.getResource("/mysql-c3p0.xml").getPath());
            cpds = new ComboPooledDataSource() ;
        }
        return cpds.getConnection();
    }

    public MySQLRSE(MySQLRSEConfig engineConfig, DrillConfig config) {

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
    }
}
