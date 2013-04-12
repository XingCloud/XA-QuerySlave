package org.apache.drill.hbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.hbase.util.HBaseEventUtils;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.hadoop.hbase.filter.Filter;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 4/11/13
 * Time: 10:32 AM
 * To change this template use File | Settings | File Templates.
 */
public class XAFilterConstructor extends FilterConstructorBase{
 
  public XAFilterConstructor(String filterCondition) throws Exception{
    super(filterCondition);
    filterCondition = filterCondition.replace("and", "&&").replace("or", "||");
    Map<String, Object> hBaseScanInfo = new HashMap<String, Object>();
    hBaseScanInfo.put("startRowKey", "21000101");
    hBaseScanInfo.put("endRowKey", "19710101");
    hBaseScanInfo.put("like", new ArrayList<String>());
    
    LogicalExpression logicalExpression = getLogicalExpressionFromString(filterCondition);
    getHBaseScanInfo(logicalExpression, hBaseScanInfo);
    
    startRowKey = (String)hBaseScanInfo.get("startRowKey");
    endRowKey = (String)hBaseScanInfo.get("endRowKey");
    List<String> rowLikes = (ArrayList<String>)hBaseScanInfo.get("rowLikes");
    
    //List<String> events = HBaseEventUtils.getSortedEvents(((String)(hBaseScanInfo.get("hBaseTableName"))).replace("xadrill", "-"), rowLikes);
    List<String> events = new ArrayList<String>(){{add("visit.*");}};
    filter = HBaseEventUtils.getRowKeyFilter(events);
  }
  
  @Override
  public String getStartRowKey(){
    return  startRowKey;  
  }
  
  @Override
  public String getEndRowKey(){
    return endRowKey;
  }
  
  
  @Override
  public Filter getFilter(){
    return filter;  
  }

  private void getHBaseScanInfo(LogicalExpression logicalExpression, Map<String, Object> hBaseScanInfo){
    if(logicalExpression instanceof FunctionCall){
      List<LogicalExpression> args = ((FunctionCall)logicalExpression).args;
      if(args.get(0) instanceof FieldReference){
        String functionName = ((FunctionCall)logicalExpression).getDefinition().getName().toLowerCase();
        String attr = ((FieldReference)args.get(0)).getRootSegment().getChild().getNameSegment().getPath().toString();
        if(attr.equals("row")){
          String value = ((ValueExpressions.QuotedString) args.get(1)).value;
          if(functionName.equals("greater than or equal to")){
            if (value.compareTo(((String)hBaseScanInfo.get("startRowKey")))<0){
              hBaseScanInfo.put("startRowKey", value);
            }
          }
          
          if(functionName.equals("less than")){
            if (value.compareTo(((String)hBaseScanInfo.get("endRowKey")))>0){
              hBaseScanInfo.put("endRowKey", value);
            }
          }
          
          if(functionName.equals("equal")){
            hBaseScanInfo.put("startRowKey",value);
            hBaseScanInfo.put("endRowKey", value);
          }
          if(functionName.equals("like")) ((ArrayList<String>)hBaseScanInfo.get("rowLikes")).add(value);
        }
      }

      if (args.get(0) instanceof FunctionCall) {
        getHBaseScanInfo(args.get(0), hBaseScanInfo);
      }
      if (args.get(1) instanceof FunctionCall) {
        getHBaseScanInfo(args.get(1), hBaseScanInfo);
      }
    }else if(logicalExpression instanceof FieldReference){
      hBaseScanInfo.put("hBaseTableName", ((FieldReference)logicalExpression).getRootSegment().getNameSegment().getPath().toString());
    }
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
}
