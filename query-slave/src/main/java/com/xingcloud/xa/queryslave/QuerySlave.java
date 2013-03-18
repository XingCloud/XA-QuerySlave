package com.xingcloud.xa.queryslave;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.drill.adhoc.AdhocSQLQueryVisitorImpl;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.optimizer.LogicalPlanOptimizer;
import org.apache.drill.exec.ref.rse.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * User: Jian Fang
 * Date: 13-3-1
 * Time: 上午11:42
 */
public class QuerySlave implements QuerySlaveProtocol{
    static final Logger logger = LoggerFactory.getLogger(QuerySlave.class);
    private RPC.Server server;

    public MapWritable query(String sql) throws IOException,JSQLParserException{
        LogicalPlan logicalPlan = null;
        DrillConfig config = DrillConfig.create();
        CCJSqlParserManager pm = new CCJSqlParserManager();
        Statement statement = pm.parse(new StringReader(sql));
        if (statement instanceof Select) {
            Select selectStatement  = (Select) statement;
            AdhocSQLQueryVisitorImpl visitor = new AdhocSQLQueryVisitorImpl();
            selectStatement.getSelectBody().accept(visitor);
            List<LogicalOperator> logicalOperators = visitor.getLogicalOperators();
            List<String> selections = visitor.getSelections();

            ObjectMapper mapper = new ObjectMapper();
            PlanProperties head = mapper.readValue(new String("{\"type\":\"apache_drill_logical_plan\",\"version\":\"1\",\"generator\":{\"type\":\"manual\",\"info\":\"na\"}}").getBytes(), PlanProperties.class);

            //List<StorageEngineConfig> storageEngines = mapper.readValue(new String("[{\"type\":\"console\",\"name\":\"console\"},{\"type\":\"fs\",\"name\":\"fs\",\"root\":\"file:///\"}]").getBytes(),new TypeReference<List<StorageEngineConfig>>() {});
            List<StorageEngineConfig> storageEngines = new ArrayList<StorageEngineConfig>();
            storageEngines.add(mapper.readValue(new String("{\"type\":\"hbase\",\"name\":\"hbase\"}").getBytes(),HBaseRSE.HBaseRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"mysql\",\"name\":\"mysql\"}").getBytes(),MySQLRSE.MySQLRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"console\",\"name\":\"console\"}").getBytes(),ConsoleRSE.ConsoleRSEConfig.class));
            storageEngines.add(mapper.readValue(new String("{\"type\":\"fs\",\"name\":\"fs\",\"root\":\"file:///\"}").getBytes(),FileSystemRSE.FileSystemRSEConfig.class));

            logicalPlan = new LogicalPlan(head, storageEngines, logicalOperators);
            IteratorRegistry ir = new IteratorRegistry();
            ReferenceInterpreter i = new ReferenceInterpreter(LogicalPlanOptimizer.getInstance().optimize(logicalPlan), ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
            i.setup();

            Collection<RunOutcome> outcomes = i.run();
            List<RecordPointer> records = i.getRecords().get(0);
            MapWritable mapWritable = new MapWritable();
            mapWritable.put(new Text("meta"),new ArrayWritable(selections.toArray(new String[selections.size()])));
            mapWritable.put(new Text("size"),new Text(String.valueOf(records.size())));
            for(RecordPointer recordPointer:records){
                mapWritable.put(new Text(String.valueOf(records.indexOf(recordPointer))), new ArrayWritable(changeToStringArray(recordPointer, selections)));
            }
            return mapWritable;
        }


        return null;

    }

    private String[] changeToStringArray(RecordPointer recordPointer, List<String> selections){
        List<String> record = new ArrayList<String>();
        for(String selection:selections){
            String string = recordPointer.getField(new FieldReference(selection)).toString();
            string = string.substring(string.indexOf("=")+1,string.indexOf("]"));
            record.add(string);
        }
        String[] _record = record.toArray(new String[record.size()]);
        return _record;
    }


    public void startServer() {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            logger.info("Starting server " + addr.getHostAddress());
            server = RPC.getServer(this, addr.getHostAddress(), 9999, 16, true, new Configuration());
            logger.info("Call queue length: " + server.getCallQueueLen());
            server.start();
            server.join();
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            logger.error("IOException when start server", e);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            logger.error("InterruptedException when start server", e);
        }
    }

    public void stopServer() {
        String host = null;

        try {
            InetAddress addr = InetAddress.getLocalHost();
            host = addr.getHostAddress();
            server.stop();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Stop server error!", e);

        }
        logger.info("Server closed.");
    }

    public static void main(String[] args) throws Exception{
        QuerySlave querySlave = new QuerySlave();
        MapWritable mapWritable = querySlave.query(new String("Select count(distinct sof-dsk_deu.uid) FROM (fix_sof-dsk INNER JOIN sof-dsk_deu ON fix_sof-dsk.uid=sof-dsk_deu.uid) WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000 and sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130102'").replace("-","xadrill"));

        for (MapWritable.Entry<Writable, Writable> entry : mapWritable.entrySet()) {
            Text key = (Text)entry.getKey();
            System.err.print(key + ":");
            if (key.toString().equals("size")){
                Text value = (Text)entry.getValue();
                System.err.println(value);
            }else{
                ArrayWritable value = (ArrayWritable)entry.getValue();
                String[] record = value.toStrings();
                System.err.println(Arrays.toString(record));
            }



        }

//        QuerySlave querySlave = new QuerySlave();
//        querySlave.startServer();
//        }

    }

}
