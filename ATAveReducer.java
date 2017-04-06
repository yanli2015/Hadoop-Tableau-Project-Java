import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class ATAveReducer  extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	  
	  @Override
	  public void reduce(Text key, Iterable<DoubleWritable> values,
		      Context context)
	      throws IOException, InterruptedException {
		  double sum = 0;
			double average = 0;
			int count = 0;
			for (DoubleWritable val : values) {
				count++;
				double temp = val.get();
				sum += temp;
				
			}
			average = sum / count;
			context.write(key, new DoubleWritable(average));
		}
	}

