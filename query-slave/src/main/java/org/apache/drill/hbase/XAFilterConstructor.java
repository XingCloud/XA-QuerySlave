package org.apache.drill.hbase;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 4/11/13
 * Time: 10:32 AM
 * To change this template use File | Settings | File Templates.
 */
public class XAFilterConstructor extends FilterConstructorBase{
  
  public XAFilterConstructor(String filterCondition){
    super(filterCondition);
  }
  
  @Override
  public String getStartRowKey(){
    return  null;  
  }
  
  @Override
  public String getEndRowKey(){
    return null;
  }
  
  
  @Override
  public Filter getFilter(){
    return null;  
  }
}
