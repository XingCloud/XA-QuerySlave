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

}
