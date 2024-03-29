package com.xingcloud.xa.queryslave;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.xingcloud.xa.queryslave.parser.PlanParser;
import com.xingcloud.xa.queryslave.optimizer.LogicalPlanOptimizer;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 3/20/13
 * Time: 8:39 AM
 * To change this template use File | Settings | File Templates.
 */
public class QuerySlaveTest {

  private boolean executePlan(String logicPlanFile) throws Exception{
    DrillConfig config = DrillConfig.create();
    BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
    config.setSinkQueues(0, queue);

    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile(logicPlanFile), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));

    i.setup();
    Collection<RunOutcome> outcomes = i.run();

    StringBuilder sb = new StringBuilder();
    while(queue.peek() != null && ! (queue.peek() instanceof RunOutcome.OutcomeType)){
      String record = new String((byte[])queue.poll());
      sb.append(record);
    }
    String result = Files.toString(FileUtils.getResourceAsFile(logicPlanFile.replace("plan","result")), Charsets.UTF_8);

    System.out.print(sb.toString());
    return sb.toString().equals(result);
  }

  private boolean executeSql(String sql) throws Exception{

    DrillConfig config = DrillConfig.create();
    BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
    config.setSinkQueues(0, queue);

    LogicalPlan logicalPlan = PlanParser.getInstance().parse(sql.replace("sof-dsk_deu","sof-dsk_deu_allversions"));
    System.out.println("Before optimize: ");
    System.out.println(logicalPlan.toJsonString(config));

    LogicalPlan optimizedPlan = LogicalPlanOptimizer.getInstance().optimize(logicalPlan);
    System.out.println("After optimize: ");
    System.out.println(optimizedPlan.toJsonString(config));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(optimizedPlan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));

    i.setup();
    Collection<RunOutcome> outcomes = i.run();

    StringBuilder sb = new StringBuilder();
    while(queue.peek() != null && ! (queue.peek() instanceof RunOutcome.OutcomeType)){
      String record = new String((byte[])queue.poll());
      sb.append(record);
    }

    System.out.print(sb.toString());

    return true; //todo
  }

  @Test
  public void testTransform() throws Exception{
   // assertTrue(executePlan("/TransformTest.plan"));
  }

//  @Test
//  public void testXX() throws Exception{
//    assertTrue(executePlan("/xx.plan"));
//  }

  @Test
  public void testSecondDaysRetained() throws Exception{
    //mysql
    //String sql = new String("Select fix_sof-dsk.uid FROM fix_sof-dsk WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000").replace("-","xadrill");

    //hbase
    //String sql = new String("Select sof-dsk_deu.uid from sof-dsk_deu where sof-dsk_deu.date='20130102' and sof-dsk_deu.l0='visit'").replace("-","xadrill");

    String sql= "Select count(distinct sof-dsk_deu.uid) " +
        "FROM (fix_sof-dsk INNER JOIN sof-dsk_deu ON fix_sof-dsk.uid=sof-dsk_deu.uid) " +
        "WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000 and sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130102'";

    //assertTrue(executeSql(sql));

  }

  @Test
  public void testMin5() throws Exception{

    //5mau 5mvisit
    String sql = "select count(0), count(distinct sof-dsk_deu.uid) " +
        "from sof-dsk_deu "+
        "where sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130225' " +
        "group by min5(sof-dsk_deu.ts)";

    //dau
    String sql2 = "select count(distinct sof-dsk_deu.uid) " +
        "from sof-dsk_deu "+
        "where sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130225'";

    //5mau
    String sql3 = "select count(0) " +
        "from sof-dsk_deu "+
        "where sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130225'";


    String sql4 = "SELECT count(distinct fix_sof-dsk.uid) FROM fix_sof-dsk   WHERE fix_sof-dsk.register_time>=20130228180000 and fix_sof-dsk.register_time<=20140101000000 and fix_sof-dsk.language >= 'it'" ;
    //assertTrue(executeSql(sql4));
  }
  
  @Test
  public void testRpc() throws Exception{
    String sql5min = "select count(0), count(distinct sof-dsk_deu.uid) " +
      "from sof-dsk_deu " +
      "where sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130225' " +
      "group by min5(sof-dsk_deu.ts)";

    String sqlSecondDaysRetained= "Select count(distinct sof-dsk_deu.uid) " +
      "FROM (fix_sof-dsk INNER JOIN sof-dsk_deu ON fix_sof-dsk.uid=sof-dsk_deu.uid) " +
      "WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000 and sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130102'";
    
    String wzj=" SELECT COUNT(DISTINCT sof-dsk_deu.uid) FROM (sof-dsk_deu INNER JOIN fix_sof-dsk ON sof-dsk_deu.uid = fix_sof-dsk.uid) WHERE sof-dsk_deu.l0 = 'visit' AND sof-dsk_deu.date >= '20130301' AND sof-dsk_deu.date <= '20130301'";
    String sql4 = "SELECT count(distinct fix_sof-dsk.uid) FROM fix_sof-dsk   WHERE fix_sof-dsk.register_time>=20130228180000 and fix_sof-dsk.register_time<=20140101000000 and fix_sof-dsk.language >= 'it'" ;
    QuerySlave querySlave = new QuerySlave();
    
    String sql = sqlSecondDaysRetained.replace("sof-dsk_deu","sof-dsk_deu_allversions");
    MapWritable mapWritable = querySlave.query(sql);


    for (MapWritable.Entry<Writable, Writable> entry : mapWritable.entrySet()) {
      Text key = (Text) entry.getKey();
      System.err.print(key + ":");
      if (key.toString().equals("size")) {
        Text value = (Text) entry.getValue();
        System.err.println(value);
      } else {
        ArrayWritable value = (ArrayWritable) entry.getValue();
        String[] record = value.toStrings();
        System.err.println(Arrays.toString(record));
      }
    }

  }
  
}
