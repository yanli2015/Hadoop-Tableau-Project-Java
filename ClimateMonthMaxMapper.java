
// cc MaxTemperatureMapper Mapper for maximum temperature example
// vv MaxTemperatureMapper
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ClimateMonthMaxMapper
  extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    try{
	  String[] split = value.toString().split(",");
		String month = split[5].substring(0, 2);
		double HOURLYDRYBULBTEMPF;
		HOURLYDRYBULBTEMPF = Double.parseDouble(split[10]);
   
      context.write(new Text(month), new DoubleWritable(HOURLYDRYBULBTEMPF));
    } catch (Exception e) {
    	
    }
    
  }
}
// ^^ MaxTemperatureMapper

