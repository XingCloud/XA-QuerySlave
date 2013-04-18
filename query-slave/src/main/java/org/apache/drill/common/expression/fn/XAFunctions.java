package org.apache.drill.common.expression.fn;

import org.apache.drill.common.expression.ArgumentValidators;
import org.apache.drill.common.expression.CallProvider;
import org.apache.drill.common.expression.FunctionDefinition;
import org.apache.drill.common.expression.OutputTypeDeterminer;
import org.apache.drill.common.expression.types.DataType;

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
        FunctionDefinition.simple("min5",new ArgumentValidators.AnyTypeAllowed(1),new OutputTypeDeterminer.SameAsFirstInput(),"min5"),
        FunctionDefinition.simple("hour",new ArgumentValidators.AnyTypeAllowed(1),new OutputTypeDeterminer.SameAsFirstInput(),"hour"),
        FunctionDefinition.operator("like",new ArgumentValidators.ComparableArguments(2), OutputTypeDeterminer.FIXED_BOOLEAN, "like"), 
        FunctionDefinition.simple("substring", new ArgumentValidators.AnyTypeAllowed(3), new OutputTypeDeterminer.SameAsFirstInput(), "substring"),
        FunctionDefinition.simple("deu_uid", new ArgumentValidators.AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput(), "deu_uid"),
        FunctionDefinition.simple("deu_event", new ArgumentValidators.AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput(), "deu_event"),
        FunctionDefinition.simple("deu_date", new ArgumentValidators.AnyTypeAllowed(1), new OutputTypeDeterminer.SameAsFirstInput(), "deu_date"),
        FunctionDefinition.simple("bit_compare", new ArgumentValidators.AnyTypeAllowed(2), new OutputTypeDeterminer.FixedType(DataType.INT32), "bit_compare")
    };
  }

}
