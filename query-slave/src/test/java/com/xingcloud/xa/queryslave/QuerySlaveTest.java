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
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collection;

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
    LogicalPlan plan = LogicalPlan.parse(config, Files.toString(FileUtils.getResourceAsFile(logicPlanFile), Charsets.UTF_8));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(plan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));

    //redirect standard output stream
    PrintStream standardOutputStream = System.out;
    ByteArrayOutputStream redirectedOutput = new ByteArrayOutputStream();
    System.setOut(new PrintStream(redirectedOutput));

    i.setup();// covert rop will use system.out
    Collection<RunOutcome> outcomes = i.run();

    redirectedOutput.write(new String("end output").getBytes());

    String output = redirectedOutput.toString();
    String result = Files.toString(FileUtils.getResourceAsFile(logicPlanFile.replace("plan","result")), Charsets.UTF_8);

    //set back to standard output stream
    System.setOut(standardOutputStream);
    System.out.println(output);

    return output.contains(result);
  }

  private boolean executeSql(String sql) throws Exception{
    sql = sql.replace("-","xadrill");
    DrillConfig config = DrillConfig.create();
    LogicalPlan logicalPlan = PlanParser.getInstance().parse(sql);
    System.out.println("Before optimize: ");
    System.out.println(logicalPlan.toJsonString(config));

    LogicalPlan optimizedPlan = LogicalPlanOptimizer.getInstance().optimize(logicalPlan);
    System.out.println("After optimize: ");
    System.out.println(optimizedPlan.toJsonString(config));
    IteratorRegistry ir = new IteratorRegistry();
    ReferenceInterpreter i = new ReferenceInterpreter(optimizedPlan, ir, new BasicEvaluatorFactory(ir), new RSERegistry(config));

    i.setup();
    Collection<RunOutcome> outcomes = i.run();


    return true; //todo
  }

  @Test
  public void testJoin() throws Exception{
    assertTrue(executePlan("/JoinTest.plan"));
  }

  @Test
  public void testDistinct() throws Exception{
    assertTrue(executePlan("/DistinctTest.plan"));
  }

  @Test
  public void testCountDistinctAggregator() throws Exception{
    assertTrue(executePlan("/CountDistinctAggTest.plan"));
  }

  @Test
  public void testXX() throws Exception{
    assertTrue(executePlan("/xx.plan"));
  }

  @Test
  public void testSecondDaysRetained() throws Exception{
    //mysql
    //String sql = new String("Select fix_sof-dsk.uid FROM fix_sof-dsk WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000").replace("-","xadrill");

    //hbase
    //String sql = new String("Select sof-dsk_deu.uid from sof-dsk_deu where sof-dsk_deu.date='20130102' and sof-dsk_deu.l0='visit'").replace("-","xadrill");

    String sql= "Select count(distinct sof-dsk_deu.uid) " +
        "FROM (fix_sof-dsk INNER JOIN sof-dsk_deu ON fix_sof-dsk.uid=sof-dsk_deu.uid) " +
        "WHERE fix_sof-dsk.register_time>=20130101000000 and fix_sof-dsk.register_time<20130102000000 and sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130102'";

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
        "where sof-dsk_deu.l0='visit' and sof-dsk_deu.date='20130225' group by min5(sof-dsk_deu.ts)";

    assertTrue(executeSql(sql3));
  }
}
