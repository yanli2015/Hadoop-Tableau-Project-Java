import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class ClimateDateMapper  extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	  
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	    try{
		  String[] split = value.toString().split(",");
			String date = split[5].substring(0, 10);
			if(split[26]!= null){
			double maxT;
			maxT = Double.parseDouble(split[26]);
	      context.write(new Text(date), new DoubleWritable(maxT));
			}
	    } catch (Exception e) {
	    	
	    }
	    
	  }
	}