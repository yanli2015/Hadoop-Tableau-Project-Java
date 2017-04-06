import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class CrimeTMapper  extends Mapper<LongWritable, Text, Text, Text> {

	  
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	    try{
		  String[] split = value.toString().split(",");
			String date = split[2].substring(0, 10);
			String type = split[5];
			
			
	      context.write(new Text(date), new Text(type));
			
	    } catch (Exception e) {
	    	
	    }
	    
	  }
	}