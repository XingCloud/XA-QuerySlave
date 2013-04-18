package org.apache.drill.hbase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.hbase.util.HBaseEventUtils;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.stringtemplate.StringTemplateGroup;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.expression.parser.ExprLexer;
import org.apache.drill.common.expression.parser.ExprParser;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.xerces.impl.dv.util.Base64;
import org.json.simple.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

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
    hBaseScanInfo.put("startRowKey", Base64.encode("21000101".getBytes()));
    hBaseScanInfo.put("endRowKey", Base64.encode("19710101".getBytes()));
    hBaseScanInfo.put("rowLikes", new ArrayList<String>());
    
    LogicalExpression logicalExpression = getLogicalExpressionFromString(filterCondition);
    getHBaseScanInfo(logicalExpression, hBaseScanInfo);
    
    startRowKey = (String)hBaseScanInfo.get("startRowKey");
    endRowKey = (String)hBaseScanInfo.get("endRowKey");

    List<String> rowLikes = (ArrayList<String>)hBaseScanInfo.get("rowLikes");
    //List<String> events = HBaseEventUtils.getSortedEvents(((String)(hBaseScanInfo.get("hBaseTableName"))).replace("xadrill", "-"), rowLikes);
    List<String> events = new ArrayList<String>(){{add("visit.");}};
    
    String startDate = HBaseEventUtils.getDateFromDEURowKey(Base64.decode(startRowKey));
    String endDate = HBaseEventUtils.getDateFromDEURowKey(Base64.decode(endRowKey));
    List<String> dates = XAFilterConstructor.getDates(startDate, endDate);
    
    filter = HBaseEventUtils.getRowKeyFilter(events, dates);
  }
  
  
  public static List<String> getDates(String startDate, String endDate){
    List<String> dates = new ArrayList<String>();
    long start = getTimestamp(startDate);
    long end = getTimestamp(endDate);
    while(start<=end){
      dates.add(getDate(start));
      start+= 24*60*60*1000;
    }
    return dates;
  }

  public static long getTimestamp(String date) {
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
    String dateString = date + " 00:00:00";
    Date nowDate = null;
    try {
      nowDate = df.parse(dateString);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    if (nowDate != null) {
      return nowDate.getTime();
    } else {
      return -1;
    }
  }
  
  public static String getDate(long timestamp){
    Date date = new Date(timestamp);
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
    return df.format(date);
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
      if (args.size()<2) return;
      String functionName = ((FunctionCall)logicalExpression).getDefinition().getName().toLowerCase();

      if(args.get(0) instanceof SchemaPath){
        String attr = ((SchemaPath)args.get(0)).getRootSegment().getChild().getNameSegment().getPath().toString();
        hBaseScanInfo.put("hBaseTableName", ((SchemaPath)args.get(0)).getRootSegment().getNameSegment().getPath().toString());
        if(attr.equals("row")){      

        }
      }else if(args.get(0) instanceof FunctionCall){
        String value;
        FunctionCall functionCall = (FunctionCall)args.get(0);
        String argFunctionName = functionCall.getDefinition().getName();
        if(functionName.equals("like")) { // deu_event(deu.row) like 'visit.'  
          value = ((ValueExpressions.QuotedString) args.get(1)).value;
          ((ArrayList<String>)hBaseScanInfo.get("rowLikes")).add(value);
        }

        if(argFunctionName.equals("bit_compare")) {
          value = ((ValueExpressions.QuotedString) functionCall.args.get(1)).value;
          if ((functionName.equals("greater than or equal to")) || (functionName.equals("equal"))){ // bit_compare(deu.row, 'xxxx')>=0
            if (Bytes.compareTo(Base64.decode(value), Base64.decode((String)hBaseScanInfo.get("startRowKey"))) <0){
              hBaseScanInfo.put("startRowKey", value);
            }
          }

          if((functionName.equals("less than")) || (functionName.equals("equal"))){ // bit_compare(deu.row, 'xxxx')<0
            if (Bytes.compareTo(Base64.decode(value), Base64.decode((String)hBaseScanInfo.get("endRowKey")))>0){
              hBaseScanInfo.put("endRowKey", value);
            }
          }
          
        }
        
      }

      if (args.get(0) instanceof FunctionCall) {
        getHBaseScanInfo(args.get(0), hBaseScanInfo);
      }
      if (args.get(1) instanceof FunctionCall) {
        getHBaseScanInfo(args.get(1), hBaseScanInfo);
      }
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
      System.out.println(e.getMessage());
      return null;
    }
  }
  
  public static void main(String[] args) throws Exception {
    Pair<byte[], byte[]> startEndRowKey = HBaseEventUtils.getStartEndRowKey("20130102","20130102", new ArrayList<String>(){{add("visit.");}}, 0, 256);
    String s1 = Base64.encode(startEndRowKey.getFirst());
    String s2 = Base64.encode(startEndRowKey.getSecond());
    System.out.println(s1+s2);
    byte[] b1 = Base64.decode(s1);
    boolean x = Arrays.equals(startEndRowKey.getFirst(), b1);
    System.out.println(x);
  }
}
