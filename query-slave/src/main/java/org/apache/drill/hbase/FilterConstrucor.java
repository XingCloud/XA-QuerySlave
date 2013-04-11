package org.apache.drill.hbase;

import org.apache.hadoop.hbase.filter.Filter;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 4/11/13
 * Time: 10:30 AM
 * To change this template use File | Settings | File Templates.
 */
public interface FilterConstrucor {
  public String getStartRowKey();
  public String getEndRowKey();
  public Filter getFilter();
}
