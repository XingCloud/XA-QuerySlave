package com.xingcloud.xa.queryslave;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.xingcloud.hbase.util.HBaseEventUtils;
import com.xingcloud.xa.queryslave.optimizer.LogicPlanMerger;
import com.xingcloud.xa.queryslave.parser.PlanParser;
import com.xingcloud.xa.queryslave.optimizer.LogicalPlanOptimizer;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionRegistry;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.drill.common.logical.JSONOptions;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.util.FileUtils;
import org.apache.drill.exec.ref.IteratorRegistry;
import org.apache.drill.exec.ref.ReferenceInterpreter;
import org.apache.drill.exec.ref.RunOutcome;
import org.apache.drill.exec.ref.eval.BasicEvaluatorFactory;
import org.apache.drill.exec.ref.rse.RSERegistry;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.xerces.impl.dv.util.Base64;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
  
  private static String sqlOnlyMysql;
  private static String sqlOnlyHBase;
  private static String sql5min;
  private static String sqlSecondDayRetained;
  private static String sqlDau;
  private static String sqlDavisit;
  
  
  @BeforeClass
  public static void constructSqls() throws IOException {

    List<String> events = HBaseEventUtils.getSortedEvents("sof-dsk", new ArrayList<String>(){{add("visit.*");}});
    Pair<byte[], byte[]> startEndRowKey = HBaseEventUtils.getStartEndRowKey("20130102","20130102", events, 0, 256);
    String startRowKey = Base64.encode(startEndRowKey.getFirst());
    String endRowKey = Base64.encode(startEndRowKey.getSecond());

    sqlSecondDayRetained = "Select count(distinct deu_uid(sof-dsk_deu.row)) " +
      "FROM (fix_sof-dsk INNER JOIN sof-dsk_deu ON fix_sof-dsk.uid=deu_uid(sof-dsk_deu.row)) " +
      "WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000 and (deu_event(sof-dsk_deu.row) like 'visit.*') " +
      "and bit_compare(sof-dsk_deu.row,'" + startRowKey+"')>=0 " +
      "and bit_compare(sof-dsk_deu.row,'"+ endRowKey+"')<0";
    
    sql5min = "select count(0), count(distinct deu_uid(sof-dsk_deu.row)) " +
      "from sof-dsk_deu "+
      "where deu_event(sof-dsk_deu.row) like 'visit.*' " +
      "and bit_compare(sof-dsk_deu.row,'" + startRowKey+ "')>=0 "+
      "and bit_compare(sof-dsk_deu.row,'"+ endRowKey + "')<0 "+
      "group by min5(sof-dsk_deu.ts)";

    sqlDau = "select count(distinct deu_uid(sof-dsk_deu.row)) " +
      "from sof-dsk_deu "+
      "where deu_event(sof-dsk_deu.row) like 'visit.*' " +
      "and bit_compare(sof-dsk_deu.row,'" + startRowKey+ "')>=0 "+
      "and bit_compare(sof-dsk_deu.row,'"+ endRowKey + "')<0 ";

    sqlDavisit = "select count(0) " +
      "from sof-dsk_deu "+
      "where deu_event(sof-dsk_deu.row) like 'visit.*' " +
      "and bit_compare(sof-dsk_deu.row,'" + startRowKey+ "')>=0 "+
      "and bit_compare(sof-dsk_deu.row,'"+ endRowKey + "')<0 ";

    sqlOnlyMysql = "Select fix_sof-dsk.uid " +
      "FROM fix_sof-dsk " +
      "WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000";

    sqlOnlyHBase ="Select deu_uid(sof-dsk_deu.row) " +
      "from sof-dsk_deu " +
      "where deu_event(sof-dsk_deu.row) like 'visit.*' " +
      "and bit_compare(sof-dsk_deu.row,'" + startRowKey+ "')>=0 "+
      "and bit_compare(sof-dsk_deu.row,'"+ endRowKey + "')<0 ";
  }
  
  private static String preProcessSql(String sql){
    return sql.replace("-","xadrill");  
  }
  
  private String sqlToLogicalPlan(String sql) throws Exception{
    DrillConfig config = DrillConfig.create();
    LogicalPlan logicalPlan = PlanParser.getInstance().parse(sql);
    return logicalPlan.toJsonString(config);
  }
  
  private boolean executePlan(String logicPlanFile, String resultFile) throws Exception{
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
    String result = Files.toString(FileUtils.getResourceAsFile(resultFile), Charsets.UTF_8);

    System.out.print(sb.toString());
    return sb.toString().equals(result);
  }

  private boolean executeSql(String... sqls) throws Exception{
    DrillConfig config = DrillConfig.create();
    BlockingQueue<Object> queue = new LinkedBlockingQueue<Object>();
    config.setSinkQueues(0, queue);

    List<LogicalPlan> logicalPlans = new ArrayList<LogicalPlan>();
    for(String sql: sqls){
      logicalPlans.add(PlanParser.getInstance().parse(preProcessSql(sql)));
    }
    LogicalPlan logicalPlan = LogicPlanMerger.getInstance().merge(logicalPlans);
    
    System.out.println(logicalPlan.toJsonString(config));

    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(logicalPlan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));

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
  public void testMysql(){
    
  }
  
  @Test
  public void testHBase(){
    
  }
  
  @Test
  public void testTransform() throws Exception{
    assertTrue(executePlan("/TransformTest.plan", "/TransformTest.result"));
  }

  @Test
  public void testSecondDaysRetained() throws Exception{
   assertTrue(executeSql(sqlSecondDayRetained));

  }

  @Test
  public void testMin5() throws Exception{
    assertTrue(executeSql(sql5min));
  }
  
  @Test
  public void testRpc() throws Exception{ 
    QuerySlave querySlave = new QuerySlave();
    MapWritable mapWritable = querySlave.query(sql5min);

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
  
  @Test
  public void testLike() throws Exception{
    String sql = "select count(distinct substring(sof-dsk_deu.row, 0, 3)) from sof-dsk_deu " +
      "where sof-dsk_deu.row>='20130205' and sof-dsk_deu.row<'20130206' " +
      "and ((sof-dsk_deu.row LIKE '%visit.*%') or (sof-dsk_deu.row LIKE '%pay%')) " +
      "and sof-dsk_deu.val.val > 10";
    
    String sql1 = "select sof-dsk_deu.row from sof-dsk_deu where (sof-dsk.row like '%visit%'  or sof-dsk_deu.row LIKE '%pay%')";
    String sql2 = "select sof-dsk_deu.row from sof-dsk_deu where sof-dsk_deu.row>='20130205' and (sof-dsk.row like '%visit%'  or sof-dsk_deu.row LIKE '%pay%')";
    System.out.println(sqlToLogicalPlan(preProcessSql(sql)));
    
    assertTrue(executePlan("/like/LikeTest.plan", "/like/LikeTest.result"));
  }
  
  @Test 
  public void testSubString() throws Exception{
    assertTrue(executePlan("/substring/SubStringTest.plan", "/substring/SubStringTest.result"));      
  }
  
  @Test
  public void testLogicalPlanMerger() throws Exception{
    //assertTrue(executeSql(sql5min, sqlSecondDayRetained));
  }
}
