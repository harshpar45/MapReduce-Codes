import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChineseDataNQ8Part2 {
	
	public static class AvgGrowthMapper extends Mapper<LongWritable, Text,DoubleWritable,Text>
	{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try{
				String []str = value.toString().split(",");
				double avg_growth = Double.parseDouble(str[4]);
				context.write(new DoubleWritable(avg_growth), value);
			}
			catch(Exception ex)
			{
				
			}
		}
	}
	
	public static class ReduceJoinReducer extends
	Reducer<DoubleWritable, Text, NullWritable, Text> {
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException 
		{
				for(Text val : values)
				{
						context.write(NullWritable.get(), val);
				}	
		}
	}
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
	    job.setJarByClass(ChineseDataNQ8Part2.class);
	    job.setMapperClass(AvgGrowthMapper.class);
	    job.setReducerClass(ReduceJoinReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
	  	job.setOutputValueClass(Text.class);
	  //	job.setNumReduceTasks();
	  	FileInputFormat.addInputPath(job, new Path(args[0]));
	  	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
