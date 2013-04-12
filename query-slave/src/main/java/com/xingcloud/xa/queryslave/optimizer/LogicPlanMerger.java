package com.xingcloud.xa.queryslave.optimizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.hbase.util.HBaseEventUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.JSONOptions;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.*;
import org.apache.drill.exec.ref.values.StringValue;
import org.json.simple.JSONObject;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 4/2/13
 * Time: 9:20 AM
 * To change this template use File | Settings | File Templates.
 */
public class LogicPlanMerger {
  
  private static LogicPlanMerger instance = new LogicPlanMerger();
  
  private LogicPlanMerger(){}
  
  public static LogicPlanMerger getInstance(){
    return instance;
  }
  
  public LogicalPlan merge(List<LogicalPlan> logicalPlans) throws Exception{
    List<LogicalOperator> hbaseScans = new ArrayList<LogicalOperator>();
    List<LogicalOperator> mysqlScans = new ArrayList<LogicalOperator>();
    List<LogicalOperator> newtLogicalOperators = new ArrayList<LogicalOperator>();

    List<String> rowLikes = new ArrayList<String>();
    String hbaseTableName = null;
    LogicalExpression allHBaseFilterExpr = null; // combine each filter expression with 'or'
    FunctionRegistry functionRegistry = new FunctionRegistry(DrillConfig.create());
      
    for (LogicalPlan _logicalPlan: logicalPlans){      
      LogicalPlan logicalPlan = LogicalPlanOptimizer.getInstance().optimizeLogicalPlanStructure(_logicalPlan);
      System.out.println(logicalPlan.toJsonString(DrillConfig.create()));
      newtLogicalOperators.addAll(logicalPlan.getSortedOperators());
      Collection<SourceOperator> sourceOperators = logicalPlan.getGraph().getSources();

        
      for(LogicalOperator logicalOperator: sourceOperators){       
        if(logicalOperator instanceof Scan){
          Scan scan = (Scan)logicalOperator;
          Filter scanFilter = getScanFilter(logicalOperator);
          if (scan.getStorageEngine().equals("hbase")){
            
            //check if we scan the same hbase table
            if(hbaseTableName == null){
              hbaseTableName = scan.getOutputReference().getPath().toString();
            }        
            if (! hbaseTableName.equals(scan.getOutputReference().getPath().toString())) throw new Exception("");
            
            //construct the default filter expression
            LogicalExpression defaultExpr;            
            if (scanFilter != null){
              defaultExpr = scanFilter.getExpr();            
            }else{
              List<LogicalExpression> args = new ArrayList<LogicalExpression>();
              args.add(functionRegistry.createExpression(">=", new FieldReference(hbaseTableName+"."+"row"), new ValueExpressions.QuotedString("19710101")));
              args.add(functionRegistry.createExpression("<", new FieldReference(hbaseTableName+"."+"row"), new ValueExpressions.QuotedString("21000101")));
              defaultExpr = functionRegistry.createExpression("and",args);
            }
            
            if (allHBaseFilterExpr == null){
              allHBaseFilterExpr = defaultExpr;
            }else{
              allHBaseFilterExpr = functionRegistry.createExpression("or", allHBaseFilterExpr, defaultExpr);
            }
            
            hbaseScans.add(scan);
          }else if(scan.getStorageEngine().equals("mysql")){
            mysqlScans.add(scan);
          }
        }
      } 
      
    }
    
    // for hbase
    if(hbaseTableName != null){
      JSONObject hBaseScanInfo = new JSONObject();
      StringBuilder sb = new StringBuilder();
      allHBaseFilterExpr.addToString(sb);
      hBaseScanInfo.put("filterCondition", sb.toString());
      hBaseScanInfo.put("filterConstructorClass", "org.apache.drill.hbase.XAFilterConstructor"); 
      ObjectMapper mapper = new ObjectMapper();
      Scan bigHbaseScan = new Scan("hbase", mapper.readValue(hBaseScanInfo.toJSONString().getBytes(), JSONOptions.class), new FieldReference(hbaseTableName));
      newtLogicalOperators.add(bigHbaseScan);
      
      //substitute the origin scan
      for(LogicalOperator scan: hbaseScans){  
        newtLogicalOperators.remove(scan);
        replaceLogicalOperator(scan, bigHbaseScan);
      }
    }
    
    //for mysql
    //newtLogicalOperators.add(newMysqlScan);
    
    return new LogicalPlan(logicalPlans.get(0).getProperties(), logicalPlans.get(0).getStorageEngines(), newtLogicalOperators);
  }
  
  public Filter getScanFilter(LogicalOperator logicalOperator){
    for(LogicalOperator subscriber: logicalOperator){
      if(subscriber instanceof Filter){
        return (Filter)subscriber;
      }
    }  
    
    return null;
  }
  
  public void replaceLogicalOperator(LogicalOperator target, LogicalOperator replacement){
    for(LogicalOperator subscriber:target.getAllSubscribers()){
      if (subscriber instanceof SingleInputOperator){        
        ((SingleInputOperator)subscriber).setInput(replacement);
      }else if(subscriber instanceof Join){
        Join join = (Join)subscriber;
        if ( target == join.getLeft()){
          join.setLeft(replacement);
        }else{
          join.setRight(replacement);
        }
      }else{
        //todo 
      }
    }       
  }
  
  
  public void merge(LogicalPlan logicalPlan, int layer){
    
    
  }
}
