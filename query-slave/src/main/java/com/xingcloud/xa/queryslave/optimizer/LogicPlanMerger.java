package com.xingcloud.xa.queryslave.optimizer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.hbase.util.HBaseEventUtils;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.logical.JSONOptions;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.*;
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

    String minStartkey = "21000101";
    String maxEndKey = "19710101";
    List<String> rowLikes = new ArrayList<String>();
    String hbaseTableName = null;
    
    for (LogicalPlan _logicalPlan: logicalPlans){      
      LogicalPlan logicalPlan = LogicalPlanOptimizer.getInstance().optimizeLogicalPlanStructure(_logicalPlan);
      newtLogicalOperators.addAll(logicalPlan.getSortedOperators());
      Collection<SourceOperator> sourceOperators = logicalPlan.getGraph().getSources();
      
      for(LogicalOperator logicalOperator: sourceOperators){       
        if(logicalOperator instanceof Scan){
          Scan scan = (Scan)logicalOperator;
          Filter originFilter = getScanFilter(logicalOperator);
          if (scan.getStorageEngine().equals("hbase")){
            //check if we scan the same hbase table
            if(hbaseTableName == null){
              hbaseTableName = scan.getOutputReference().getPath().toString();
            }
            if (! hbaseTableName.equals(scan.getOutputReference().getPath().toString())) throw new Exception("");
            
            String starKey = "19710101";
            String endKey ="21000101";
            List<String> _rowLikes = new ArrayList<String>();
            
            if (originFilter != null){
              Map<String, Object> hbaseScanInfo = new HashMap<String, Object>();
              hbaseScanInfo.put("rowLikes", new ArrayList<String>());
              getHBaseScanInfo(originFilter.getExpr(), hbaseScanInfo);
              starKey = (String)hbaseScanInfo.get("startKey");
              endKey = (String)hbaseScanInfo.get("endKey");
              _rowLikes = (List<String>)hbaseScanInfo.get("rowLikes");
            }
            
            if(starKey.compareTo(minStartkey)<0) minStartkey = starKey;
            if(endKey.compareTo(maxEndKey)>0) maxEndKey = endKey;
            rowLikes.addAll(_rowLikes);
            hbaseScans.add(scan);
          }else if(scan.getStorageEngine().equals("mysql")){
            mysqlScans.add(scan);
          }
        }
      } 
      
    }
    
    // for hbase
    if(hbaseTableName != null){
      ObjectMapper mapper = new ObjectMapper();
      JSONObject hbaseScanInfo = new JSONObject();
      hbaseScanInfo.put("startKey", minStartkey);
      hbaseScanInfo.put("endkey", maxEndKey);
      //List<String> events = HBaseEventUtils.getSortedEvents(hbaseTableName.replace("xadrill", "-"), rowLikes);
      List<String> events = new ArrayList<String>(){{add("a");add("b");}};
      hbaseScanInfo.put("filter", HBaseEventUtils.getRowKeyFilter(events));
      
      Scan bigHbaseScan = new Scan("hbase", mapper.readValue(hbaseScanInfo.toJSONString().getBytes(), JSONOptions.class), new FieldReference(hbaseTableName));
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

  private void getHBaseScanInfo(LogicalExpression logicalExpression, Map<String, Object> hbaseScanInfo){
    if(logicalExpression instanceof FunctionCall){
      List<LogicalExpression> args = ((FunctionCall)logicalExpression).args;
      if(args.get(0) instanceof FieldReference){
        String functionName = ((FunctionCall)logicalExpression).getDefinition().getName().toLowerCase();
        String attr = ((FieldReference)args.get(0)).getRootSegment().getChild().getNameSegment().getPath().toString();
        if(attr.equals("row")){
          String value = ((ValueExpressions.QuotedString) args.get(1)).value;
          if(functionName.equals("greater than or equal to")) hbaseScanInfo.put("startKey", value);
          if(functionName.equals("less than")) hbaseScanInfo.put("endKey", value);
          if(functionName.equals("equal")){
            hbaseScanInfo.put("startKey",value);
            hbaseScanInfo.put("endKey", value);
          }
          if(functionName.equals("like")) ((ArrayList<String>)hbaseScanInfo.get("rowLikes")).add(value);
        }
      }

      if (args.get(0) instanceof FunctionCall) {
        getHBaseScanInfo(args.get(0), hbaseScanInfo);
      }
      if (args.get(1) instanceof FunctionCall) {
        getHBaseScanInfo(args.get(1), hbaseScanInfo);
      }
    }  
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
