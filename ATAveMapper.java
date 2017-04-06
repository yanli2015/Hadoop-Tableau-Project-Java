import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class ATAveMapper  extends Mapper<LongWritable, Text, Text, DoubleWritable> {  
	  
	  @Override
	  public void map(LongWritable key, Text value, Context context)
	      throws IOException, InterruptedException {
	    try{
		  String[] split = value.toString().split(",");
			String month = split[5].substring(0, 2);
			double VaporPressure =1.0;
			double at;
			at = -2.7 + (1.04 *Double.parseDouble(split[11]) )+ 
				(2.0*(Double.parseDouble(split[16]))*VaporPressure) -(0.65*Double.parseDouble(split[17]));
	      context.write(new Text(month), new DoubleWritable(at));
	      
	    } catch (Exception e) {
	    	
	    }
	    
	  }}
	  
	 