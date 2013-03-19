package com.xingcloud.xa.queryslave.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.xa.queryslave.optimizer.LogicalPlanOptimizer;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.OperatorGraph;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.LogicalOperator;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-2-20
 * Time: 上午11:20
 * To change this template use File | Settings | File Templates.
 */
public class PlanParser {

    //static final Logger logger = LoggerFactory.getLogger(RunSimplePlan.class);
    private CCJSqlParserManager pm = new CCJSqlParserManager();
    private static PlanParser instance = new PlanParser();

    private PlanParser() {

    }

    public static PlanParser getInstance() {
        return instance;
    }

    public LogicalPlan parse(String sql) throws JSQLParserException, Exception {
        LogicalPlan plan = null;
        Statement statement = pm.parse(new StringReader(sql));
        if (statement instanceof Select) {
            Select selectStatement  = (Select) statement;
            AdhocSQLQueryVisitorImpl visitor = new AdhocSQLQueryVisitorImpl();
            selectStatement.getSelectBody().accept(visitor);
            List<LogicalOperator> logicalOperators = visitor.getLogicalOperators();

            ObjectMapper mapper = new ObjectMapper();
            PlanProperties head = mapper.readValue(new String("{\"type\":\"apache_drill_logical_plan\",\"version\":\"1\",\"generator\":{\"type\":\"manual\",\"info\":\"na\"}}").getBytes(), PlanProperties.class);

            //List<StorageEngineConfig> storageEngines = mapper.readValue(new String("[{\"type\":\"console\",\"name\":\"console\"},{\"type\":\"fs\",\"name\":\"fs\",\"root\":\"file:///\"}]").getBytes(),new TypeReference<List<StorageEngineConfig>>() {});
            List<StorageEngineConfig> storageEngines = new ArrayList<StorageEngineConfig>();
            storageEngines.add(mapper.readValue(new String("{\"type\":\"hbase\",\"name\":\"hbase\"}").getBytes(),HBaseRSE.HBaseRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"mysql\",\"name\":\"mysql\"}").getBytes(),MySQLRSE.MySQLRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"console\",\"name\":\"console\"}").getBytes(),ConsoleRSE.ConsoleRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"fs\",\"name\":\"fs\",\"root\":\"file:///\"}").getBytes(),FileSystemRSE.FileSystemRSEConfig.class));

            plan = new LogicalPlan(head, storageEngines, logicalOperators);
        }
        return plan;
    }

    public static void test(){
        SchemaPath schemaPath = new SchemaPath("sof-dsk_deu");


    }
    public static void main(String[] args) throws Exception{
        //logger.info("xxx");
        DrillConfig config = DrillConfig.create();


        //liucun
        String sql= new String("Select count(distinct sof-dsk_deu.uid) FROM (fix_sof-dsk INNER JOIN sof-dsk_deu ON fix_sof-dsk.uid=sof-dsk_deu.uid) WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000 and sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130102'").replace("-","xadrill");

        //mysql
        //String sql = new String("select fix_sof-dsk.uid from fix_sof-dsk").replace("-","xadrill");
        //String sql = new String("Select fix_sof-dsk.uid FROM fix_sof-dsk WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000").replace("-","xadrill");

        //hbase
        //String sql = new String("Select sof-dsk_deu.uid from sof-dsk_deu where sof-dsk_deu.date='20130102' and sof-dsk_deu.l0='visit'").replace("-","xadrill");

        String sql2= new String("Select sof-dsk_deu.uid FROM sof-dsk_deu WHERE sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130102'").replace("-","xadrill");
        String sql3= new String("Select fix_sof-dsk.uid FROM fix_sof-dsk WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000").replace("-","xadrill");

        LogicalPlan logicalPlan = new PlanParser().parse(sql);
        System.out.println("Before optimize: ");
        System.out.println(logicalPlan.toJsonString(config));

        LogicalPlan optimizedPlan = LogicalPlanOptimizer.getInstance().optimize(logicalPlan);
        System.out.println("After optimize: ");
        System.out.println(optimizedPlan.toJsonString(config));
        IteratorRegistry ir = new IteratorRegistry();
        ReferenceInterpreter i = new ReferenceInterpreter(optimizedPlan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));

        i.setup();
        Collection<RunOutcome> outcomes = i.run();

    }
}
