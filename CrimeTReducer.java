import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class CrimeTReducer extends Reducer<Text, Text, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	  public void reduce(Text key, Iterable<Text> values,
		      Context context)
	      throws IOException, InterruptedException {
		  int count = 0;
			for (Text val : values) {
				count++;
			}
			result.set(count);
			context.write(key, result);
			
			
		}
	}
