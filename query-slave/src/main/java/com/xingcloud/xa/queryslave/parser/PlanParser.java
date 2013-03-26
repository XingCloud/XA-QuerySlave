package com.xingcloud.xa.queryslave.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.ref.rse.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-20
 * Time: 上午11:20
 * To change this template use File | Settings | File Templates.
 */
public class PlanParser {

    static final Logger logger = LoggerFactory.getLogger(PlanParser.class);
    private CCJSqlParserManager pm = new CCJSqlParserManager();
    private static PlanParser instance = new PlanParser();
    private List<String> selections = null;
    private PlanParser() {

    }

    public static PlanParser getInstance() {
        return instance;
    }

    public List<String> getSelections(){
        return selections;
    }

    public LogicalPlan parse(String sql) throws JSQLParserException, Exception {
        sql = sql.replace("-","xadrill");
        LogicalPlan plan = null;
        Statement statement = pm.parse(new StringReader(sql));
        if (statement instanceof Select) {
            Select selectStatement  = (Select) statement;
            AdhocSQLQueryVisitorImpl visitor = new AdhocSQLQueryVisitorImpl();
            selectStatement.getSelectBody().accept(visitor);
            List<LogicalOperator> logicalOperators = visitor.getLogicalOperators();
            selections = visitor.getSelections();

            ObjectMapper mapper = new ObjectMapper();
            PlanProperties head = mapper.readValue(new String("{\"type\":\"apache_drill_logical_plan\",\"version\":\"1\",\"generator\":{\"type\":\"manual\",\"info\":\"na\"}}").getBytes(), PlanProperties.class);

            List<StorageEngineConfig> storageEngines = new ArrayList<StorageEngineConfig>();
            storageEngines.add(mapper.readValue(new String("{\"type\":\"hbase\",\"name\":\"hbase\"}").getBytes(),HBaseRSE.HBaseRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"mysql\",\"name\":\"mysql\"}").getBytes(),MySQLRSE.MySQLRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"console\",\"name\":\"console\"}").getBytes(),ConsoleRSE.ConsoleRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"fs\",\"name\":\"fs\",\"root\":\"file:///\"}").getBytes(),FileSystemRSE.FileSystemRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"queue\",\"name\":\"queue\"}").getBytes(), QueueRSE.QueueRSEConfig.class));

            plan = new LogicalPlan(head, storageEngines, logicalOperators);
        }
        return plan;
    }

    public static void test(){
        SchemaPath schemaPath = new SchemaPath("sof-dsk_deu");


    }
    public static void main(String[] args) throws Exception{

    }
}
