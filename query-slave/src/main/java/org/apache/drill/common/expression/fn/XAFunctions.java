package org.apache.drill.common.expression.fn;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 3/20/13
 * Time: 9:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class XAFunctions implements CallProvider{
  @Override
  public FunctionDefinition[] getFunctionDefintions() {
    return new FunctionDefinition[]{
        FunctionDefinition.aggregator("countDistinct",  new ArgumentValidators.AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput()),
        FunctionDefinition.operator("min5",new ArgumentValidators.AnyTypeAllowed(1),new OutputTypeDeterminer.SameAsFirstInput(),"min5"),
        FunctionDefinition.operator("hour",new ArgumentValidators.AnyTypeAllowed(1),new OutputTypeDeterminer.SameAsFirstInput(),"hour")
    };
  }

  public static FunctionDefinition getFunctionDefintion(String name){

    Map<String, Integer> funtionMap = new HashMap<String,Integer>();
    funtionMap.put("countDistinct",0);
    funtionMap.put("min5",1);
    funtionMap.put("hour",2);

    return new XAFunctions().getFunctionDefintions()[funtionMap.get(name)];
  }
}
