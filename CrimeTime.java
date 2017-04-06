


	import java.io.IOException;
	import java.util.StringTokenizer;

	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.DoubleWritable;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.Reducer.Context;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	public class CrimeTime {
		public static class TokenizerMapper extends
				Mapper<Object, Text, Text, IntWritable> {
			private final static IntWritable one = new IntWritable(1);
			private Text crimeTime = new Text();

			public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException {
				 try{
				 String[] split = value.toString().split(",");
				 String temp = split[2];
				 String[] time = temp.split(" ");
				 String hour = time[1].substring(0,2);
				 crimeTime.set(hour);
				 context.write(crimeTime, one);
				 } catch (Exception e) {
				 System.out.print("erro---------------------" + e.toString());
				 }
				
				
			}
		}

		public static class SumReducer extends
				Reducer<Text, IntWritable, Text, IntWritable> {
			private IntWritable result = new IntWritable();

			public void reduce(Text key, Iterable<IntWritable> values,
					Context context) throws IOException, InterruptedException {
				int sum = 0;
				int count = 0;
				for (IntWritable val : values) {
					sum += val.get();
					count ++;
				}
				result.set(sum/count);
				context.write(key, result);
			}
		}

		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "CrimeMonth.");
			job.setJarByClass(CrimeTime.class);
			job.setJar("CrimeTime.jar");
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(SumReducer.class);
			job.setReducerClass(SumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

