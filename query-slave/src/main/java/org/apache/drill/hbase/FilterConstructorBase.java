package org.apache.drill.hbase;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 4/11/13
 * Time: 10:32 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract  class FilterConstructorBase implements FilterConstrucor{
  protected String startRowKey;
  protected String endRowKey;
  protected Filter filter;
  protected String filterCondition;
  public FilterConstructorBase(String filterCondition){
    this.filterCondition = filterCondition;  
  }
}
