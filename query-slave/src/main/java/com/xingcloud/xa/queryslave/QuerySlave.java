package com.xingcloud.xa.queryslave;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.basic.remote.QuerySlaveProtocol;
import com.xingcloud.xa.queryslave.optimizer.LogicalPlanOptimizer;
import com.xingcloud.xa.queryslave.parser.PlanParser;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;
import org.apache.drill.exec.ref.values.SimpleArrayValue;
import org.apache.drill.exec.ref.values.SimpleMapValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.ipc.RPC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * User: Jian Fang
 * Date: 13-3-1
 * Time: 上午11:42
 */
public class QuerySlave implements QuerySlaveProtocol {
    static final Logger logger = LoggerFactory.getLogger(QuerySlave.class);
    private RPC.Server server;

    public MapWritable query(String sql) throws Exception{
        logger.info(sql);
        LogicalPlan logicalPlan = PlanParser.getInstance().parse(sql);
        LogicalPlan optimizeLogicalPlan = LogicalPlanOptimizer.getInstance().optimize(logicalPlan);
        System.out.println(optimizeLogicalPlan.toJsonString(DrillConfig.create()));
        if (logicalPlan != null){
            DrillConfig config = DrillConfig.create();
            BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
            config.setSinkQueues(0, queue);

            IteratorRegistry ir = new IteratorRegistry();
            ReferenceInterpreter i = new ReferenceInterpreter( optimizeLogicalPlan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
            i.setup();
            Collection<RunOutcome> outcomes = i.run();

            //construct the map writable
            MapWritable mapWritable = new MapWritable();
            List<String> selections = PlanParser.getInstance().getSelections();
            if (sql.contains("group")){
              selections.add("segmentvalue");
            }

            mapWritable.put(new Text("size"),new Text(String.valueOf(queue.size()-1)));
            ObjectMapper mapper = new ObjectMapper();
            int index = 0;
            while(queue.peek() != null && ! (queue.peek() instanceof RunOutcome.OutcomeType)){
                String record = new String((byte[])queue.poll());
                JsonNode jsonNode = mapper.readValue(record.getBytes(), JsonNode.class);
                DataValue dataValue = convert(jsonNode);
                mapWritable.put(new Text(String.valueOf(index)), new ArrayWritable(changeToStringArray(dataValue, selections)));
                index++;
            }

            for(int j=0; j<selections.size();j++){
              String metaString = selections.get(j);
              if (selections.get(j).contains("countDistinct")){
                metaString = metaString.replace("countDistinct.", "count.distinct ").replaceFirst("\\.", "(").replace("xadrill", "-") + ")";
              }else if (selections.get(j).contains("count") || selections.get(j).contains("sum")){
                metaString = selections.get(j).replaceFirst("\\.", "(").replace("xadrill", "-") + ")";
              }
              selections.set(j, metaString);
            }
            mapWritable.put(new Text("meta"),new ArrayWritable(selections.toArray(new String[selections.size()])));
            return mapWritable;
          }
          return null;

    }

    //copy from JSONRecordReader
    private DataValue convert(JsonNode node) {
      if (node == null || node.isNull() || node.isMissingNode()) {
        return DataValue.NULL_VALUE;
      } else if (node.isArray()) {
        SimpleArrayValue arr = new SimpleArrayValue(node.size());
        for (int i = 0; i < node.size(); i++) {
          arr.addToArray(i, convert(node.get(i)));
        }
        return arr;
      } else if (node.isObject()) {
        SimpleMapValue map = new SimpleMapValue();
        String name;
        for (Iterator<String> iter = node.fieldNames(); iter.hasNext();) {
          name = iter.next();
          map.setByName(name, convert(node.get(name)));
        }
        return map;
      } else if (node.isBinary()) {
        try {
          return new ScalarValues.BytesScalar(node.binaryValue());
        } catch (IOException e) {
          throw new RuntimeException("Failure converting binary value.", e);
        }
      } else if (node.isBigDecimal()) {
        throw new UnsupportedOperationException();
  //      return new BigDecimalScalar(node.decimalValue());
      } else if (node.isBigInteger()) {
        throw new UnsupportedOperationException();
  //      return new BigIntegerScalar(node.bigIntegerValue());
      } else if (node.isBoolean()) {
        return new ScalarValues.BooleanScalar(node.asBoolean());
      } else if (node.isFloatingPointNumber()) {
        if (node.isBigDecimal()) {
          throw new UnsupportedOperationException();
  //        return new BigDecimalScalar(node.decimalValue());
        } else {
          return new ScalarValues.DoubleScalar(node.asDouble());
        }
      } else if (node.isInt()) {
        return new ScalarValues.IntegerScalar(node.asInt());
      } else if (node.isLong()) {
        return new ScalarValues.LongScalar(node.asLong());
      } else if (node.isTextual()) {
        return new ScalarValues.StringScalar(node.asText());
      } else {
        throw new UnsupportedOperationException(String.format("Don't know how to convert value of type %s.", node
            .getClass().getCanonicalName()));
      }

    }

    private String[] changeToStringArray(DataValue dataValue, List<String> selections){
        SimpleMapValue simpleMapValue = (SimpleMapValue)dataValue;
        List<String> record = new ArrayList<String>();
        for(String selection:selections){
            String string = simpleMapValue.getValue(new FieldReference(selection).getRootSegment()).toString();
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
        querySlave.startServer();
    }

  @Override
  public long getProtocolVersion(String s, long l) throws IOException {
    return 1;
  }
}
