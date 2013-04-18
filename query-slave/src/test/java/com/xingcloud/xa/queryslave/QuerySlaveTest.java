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
import org.junit.Test;
import static org.junit.Assert.*;

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
  
  private boolean sqlToPlan(String sql) throws Exception{
    DrillConfig config = DrillConfig.create();
    LogicalPlan logicalPlan = PlanParser.getInstance().parse(sql.replace("sof-dsk_deu", "sof-dsk_deu_allversions"));
    System.out.println(logicalPlan.toJsonString(config));
    return true;
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
      logicalPlans.add(PlanParser.getInstance().parse(sql.replace("sof-dsk_deu", "sof-dsk_deu_allversions")));
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
  public void testOnlyMysql(){
    
  }
  
  @Test
  public void testTransform() throws Exception{
    assertTrue(executePlan("/TransformTest.plan", "/TransformTest.result"));
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

//    String sql= "Select count(distinct sof-dsk_deu.uid) " +
//        "FROM (fix_sof-dsk INNER JOIN sof-dsk_deu ON fix_sof-dsk.uid=sof-dsk_deu.uid) " +
//        "WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000 and sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130102'";

    Pair<byte[], byte[]> startEndRowKey = HBaseEventUtils.getStartEndRowKey("20130102","20130102", new ArrayList<String>(){{add("visit.");}}, 0, 256);
    String starRowKey = Base64.encode(startEndRowKey.getFirst());
    String endRowKey = Base64.encode(startEndRowKey.getSecond());
        
    String sql= "Select count(distinct deu_uid(sof-dsk_deu.row)) " +
      "FROM (fix_sof-dsk INNER JOIN sof-dsk_deu ON fix_sof-dsk.uid=deu_uid(sof-dsk_deu.row)) " +
      "WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000 and (deu_event(sof-dsk_deu.row) like 'visit.') " +
      "and bit_compare(sof-dsk_deu.row,'" +
      starRowKey+
      "')>=0 and bit_compare(sof-dsk_deu.row,'"+
      endRowKey+"')<0";
   assertTrue(executeSql(sql));

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
  
  @Test
  public void testLike() throws Exception{
    String sql = "select count(distinct substring(sof-dsk_deu.row, 0, 3)) from sof-dsk_deu " +
      "where sof-dsk_deu.row>='20130205' and sof-dsk_deu.row<'20130206' " +
      "and ((sof-dsk_deu.row LIKE '%visit.*%') or (sof-dsk_deu.row LIKE '%pay%')) " +
      "and sof-dsk_deu.val.val > 10";
    
    String sql1 = "select sof-dsk_deu.row from sof-dsk_deu where (sof-dsk.row like '%visit%'  or sof-dsk_deu.row LIKE '%pay%')";
    String sql2 = "select sof-dsk_deu.row from sof-dsk_deu where sof-dsk_deu.row>='20130205' and (sof-dsk.row like '%visit%'  or sof-dsk_deu.row LIKE '%pay%')";
    sqlToPlan(sql);
    
    assertTrue(executePlan("/like/LikeTest.plan", "/like/LikeTest.result"));
  }
  
  @Test 
  public void testSubString() throws Exception{
    assertTrue(executePlan("/substring/SubStringTest.plan", "/substring/SubStringTest.result"));      
  }
  
  @Test
  public void getUid() throws Exception{
    
  }
  
  @Test
  public void testLogicalMerger() throws Exception{

    String sql= "Select count(distinct substring(sof-dsk_deu.row, 0,3)) " +
      "FROM (fix_sof-dsk INNER JOIN sof-dsk_deu ON fix_sof-dsk.uid=substring(sof-dsk_deu.row, 0, 3)) " +
      "WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000 and sof-dsk_deu.row like 'visit' and sof-dsk_deu.row='20130225'";
    
    String sql1 = "select count(distinct substring(sof-dsk_deu.row, 0, 3)) " +
      "from sof-dsk_deu "+
      "where sof-dsk_deu.row like '%visit.*%' and sof-dsk_deu.row>='20130226' and sof-dsk_deu.row<'20130227'" +
      "group by min5(sof-dsk_deu.ts)";
    
    String sql2 = "select sof-dsk_deu.row from sof-dsk_deu";
    
    List<LogicalPlan> logicalPlans = new ArrayList<LogicalPlan>();
    logicalPlans.add(PlanParser.getInstance().parse(sql.replace("sof-dsk_deu", "sof-dsk_deu_allversions")));
    logicalPlans.add(PlanParser.getInstance().parse(sql1.replace("sof-dsk_deu","sof-dsk_deu_allversions")));
    logicalPlans.add(PlanParser.getInstance().parse(sql2.replace("sof-dsk_deu","sof-dsk_deu_allversions")));

    DrillConfig config = DrillConfig.create();
    LogicalPlan logicalPlan = LogicPlanMerger.getInstance().merge(logicalPlans);
    System.out.println(logicalPlan.toJsonString(config));
    
    //construct rop
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(logicalPlan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));
    i.setup();
  }

  private LogicalExpression getLogicalExpressionFromString(String expr){
    try{
      ExprLexer lexer = new ExprLexer(new ANTLRStringStream(expr));
      CommonTokenStream tokens = new CommonTokenStream(lexer);
      ExprParser parser = new ExprParser(tokens);
      parser.setRegistry(new FunctionRegistry(DrillConfig.create()));
      ExprParser.parse_return ret = parser.parse();
      return ret.e;
    }catch (Exception e){
      return null;
    }
  }
  @Test
  public void testRemoveExpression(){
    String stringExpr = "fix_sof-dsk.register_time>=1 && (fix_sof-dsk.register_time<2 && sof-dsk_deu.row>3)";
    //stringExpr = "( ( ( ('fix_sofxadrilldsk.register_time')  >= (20130101000000) )  && ( ('fix_sofxadrilldsk.register_time')  < (20130102000000) ) )  && ( ('sofxadrilldsk_deu_allversions.row')  like (\"visit\") ) )  && ( ('sofxadrilldsk_deu_allversions.row')  == (\"20130225\") ) ";
    stringExpr = stringExpr.replace("-", "xadrill");
    LogicalExpression expr = getLogicalExpressionFromString(stringExpr);
    StringBuilder sb1 = new StringBuilder();
    expr.addToString(sb1);
    System.out.println(sb1.toString());
    
    Pair<Boolean, LogicalExpression> result=  LogicalPlanOptimizer.getInstance().removeExtraExpression(expr, "sofxadrilldsk_deu");
    StringBuilder sb = new StringBuilder();
    System.out.println(result.getFirst().toString());
    result.getSecond().addToString(sb);
    System.out.println(sb.toString());
  }
}
