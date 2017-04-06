import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class ClimateDateReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();
	  @Override
	  public void reduce(Text key, Iterable<DoubleWritable> values,
		      Context context)
	      throws IOException, InterruptedException {
		  
	  
		  double sum = 0;
			double average = 0;
			//int count 
			double min = Double.MAX_VALUE;
			for (DoubleWritable val : values) {
				double temp = val.get();
				min = Math.min(min, temp);
			}
			result.set(min);
			context.write(key, result);
			
			
		}
	}
