package org.apache.drill.exec.ref.eval.fn;

import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.EvaluatorTypes;
import org.apache.drill.exec.ref.eval.fn.agg.CountAggregator;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.ScalarValues;

import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 3/21/13
 * Time: 2:49 AM
 * To change this template use File | Settings | File Templates.
 */

@FunctionEvaluator("countDistinct")
public class CountDistinctAggregator implements EvaluatorTypes.AggregatingEvaluator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CountDistinctAggregator.class);

  long l = 0;
  boolean checkForNull = false;
  EvaluatorTypes.BasicEvaluator child;
  boolean isConstant;
  private Set<Integer> duplicate = new HashSet<Integer>();

  public CountDistinctAggregator(RecordPointer recordPointer, FunctionArguments e){
    isConstant = e.isOnlyConstants();
    if(!e.getOnlyEvaluator().isConstant()){
      checkForNull = true;
      child = e.getOnlyEvaluator();
    }
  }

  @Override
  public void addRecord() {
    if(checkForNull){
      DataValue dataValue = child.eval();
      if((dataValue != DataValue.NULL_VALUE) && (!duplicate.contains(dataValue.hashCode()))){
        l++;
        duplicate.add(dataValue.hashCode());
      }
    }else{
      l++;
    }
  }

  @Override
  public DataValue eval() {
    duplicate.clear();
    DataValue v = new ScalarValues.LongScalar(l);
    l = 0;
    return v;
  }

  @Override
  public boolean isConstant() {
    return isConstant;
  }
}
