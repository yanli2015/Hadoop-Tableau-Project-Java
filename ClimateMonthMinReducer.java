import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class ClimateMonthMinReducer 
	 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		  
		  public void reduce(Text key, Iterable<DoubleWritable> values,
		      Context context)
		      throws IOException, InterruptedException {
		    
		    double minValue = Double.MAX_VALUE;
		    for (DoubleWritable value : values) {
		      minValue = Math.min(minValue, value.get());
		    }
		    context.write(key, new DoubleWritable(minValue));
		  }
		}
