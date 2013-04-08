package org.apache.drill.exec.ref.eval.fn;

import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.eval.BaseBasicEvaluator;
import org.apache.drill.exec.ref.eval.EvaluatorTypes;
import org.apache.drill.exec.ref.values.DataValue;
import org.apache.drill.exec.ref.values.NumericValue;
import org.apache.drill.exec.ref.values.ScalarValues;
import org.apache.drill.exec.ref.values.StringValue;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Created with IntelliJ IDEA.
 * User: wangchangli
 * Date: 3/20/13
 * Time: 9:52 AM
 * To change this template use File | Settings | File Templates.
 */
public class XAEvaluator {

  @FunctionEvaluator("min5")
  public static class Min5Evaluator extends BaseBasicEvaluator {
    private final EvaluatorTypes.BasicEvaluator args[];

    public Min5Evaluator(RecordPointer record, FunctionArguments args){
      super(args.isOnlyConstants(), record);
      this.args = args.getArgsAsArray();
    }

    @Override
    public  StringValue eval() {
      DataValue v = args[0].eval();
      return new ScalarValues.StringScalar(getKeyBySpecificPeriod(v.getAsNumeric().getAsLong(), 5));
    }
  }

  @FunctionEvaluator("hour")
  public static class HourEvaluator extends BaseBasicEvaluator {
    private final EvaluatorTypes.BasicEvaluator args[];

    public HourEvaluator(RecordPointer record, FunctionArguments args){
      super(args.isOnlyConstants(), record);
      this.args = args.getArgsAsArray();
    }

    @Override
    public StringValue eval() {
      DataValue v = args[0].eval();
      return new ScalarValues.StringScalar(getKeyBySpecificPeriod(v.getAsNumeric().getAsLong(), 60));
    }

  }

  public static  String getKeyBySpecificPeriod(long timestamp, int period){
    String ID = "GMT+8";
    TimeZone tz = TimeZone.getTimeZone(ID);
    SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    sf.setTimeZone(tz);

    String[] yhm = sf.format(timestamp).split(" ");
    String[] hm = yhm[1].split(":");
    int minutes = Integer.parseInt(hm[1]);

    int val = minutes % period;
    if (val != 0) {
      minutes = minutes - val;
    }

    String minutesStr = String.valueOf(minutes);
    if (minutes < 10) {
      minutesStr = "0" + minutesStr;
    }

    return yhm[0] + " " + hm[0] + ":" + minutesStr;
  }
  
  @FunctionEvaluator("like")
  public static class Like extends BaseBasicEvaluator{
    private final EvaluatorTypes.BasicEvaluator left;
    private final EvaluatorTypes.BasicEvaluator right;
    
    public  Like(RecordPointer record, FunctionArguments args){
      super(args.isOnlyConstants(), record);
      left = args.getEvaluator(0);
      right = args.getEvaluator(1);
    }
    
    @Override
    public ScalarValues.BooleanScalar eval(){
      String left =this.left.eval().getAsStringValue().getString().toString();
      String right= this.right.eval().getAsStringValue().getString().toString();
      right = right.replace(".", "\\.").replace("?", ".").replace("%", ".*");
      return new ScalarValues.BooleanScalar(left.matches(right));
    }
  }
  
  @FunctionEvaluator("substring")
  public static class SubString extends BaseBasicEvaluator{
    private final EvaluatorTypes.BasicEvaluator target;
    private final EvaluatorTypes.BasicEvaluator beginIndex;
    private final EvaluatorTypes.BasicEvaluator endIndex;
    
    public SubString(RecordPointer record, FunctionArguments args){
      super(args.isOnlyConstants(), record);   
      target = args.getEvaluator(0);
      beginIndex = args.getEvaluator(1);
      endIndex = args.getEvaluator(2);
    }
    
    @Override
    public StringValue eval(){
      return new ScalarValues.StringScalar(target.eval().getAsStringValue().getString().toString().substring((int)beginIndex.eval().getAsNumeric().getAsLong(), (int)endIndex.eval().getAsNumeric().getAsLong()));      
    }
  }
}
