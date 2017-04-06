import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class CrimeTheftMapper extends Mapper<Text, Text, Text, Text> {  
	  
	  public void map(Object key, Text value, Context context)
	      throws IOException, InterruptedException {
	    
		  String[] split = value.toString().split("\t+");
			String month = split[3].substring(0, 2);
			String crimeType = split[5];
	       context.write(new Text(month), new Text(crimeType));
	   
	    	
	    
	   
	    
	  }}
	  
	 