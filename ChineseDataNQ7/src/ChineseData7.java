import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChineseData7 {

	public static class D01Mapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(";");
			context.write(new Text(parts[5]), new Text("D01," + parts[8]));
		}
	}

	public static class D02Mapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(";");
			context.write(new Text(parts[5]), new Text("D02," + parts[8]));
		}
	}
	public static class D11Mapper extends
	Mapper<LongWritable, Text, Text, Text> {
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String record = value.toString();
	String[] parts = record.split(";");
	context.write(new Text(parts[5]), new Text("D11," + parts[8]));
}
}
	public static class D12Mapper extends
	Mapper<LongWritable, Text, Text, Text> {
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String record = value.toString();
	String[] parts = record.split(";");
	context.write(new Text(parts[5]), new Text("D12," + parts[8]));
}
}

	public static class ReduceJoinReducer extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//String name = "";
			double total = 0.0,total2=0.0,total3=0.0,total4=0.0;
			//int count = 0;
			for (Text t : values) {
				String parts[] = t.toString().split(",");
				if (parts[0].equals("D01")) {
					//count++;
					total += Float.parseFloat(parts[1]);
				} 
				else if (parts[0].equals("D02")) {
					total2 += Float.parseFloat(parts[1]);
					}
				else if (parts[0].equals("D11")) {
					total3 += Float.parseFloat(parts[1]);
			}
				else if (parts[0].equals("D12")) {
					total4 += Float.parseFloat(parts[1]);
			}
				
				}
			
			String str = String.format("%f\t%f\t%f\t%f",total3 ,total4,total,total2 );
			context.write(new Text(key), new Text(str));
		
	}
	}

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
	    job.setJarByClass(ChineseData7.class);
	    job.setJobName("Reduce Side Join");
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, D01Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, D02Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]),TextInputFormat.class, D11Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[3]),TextInputFormat.class, D12Mapper.class);
		
		Path outputPath = new Path(args[4]);
		FileOutputFormat.setOutputPath(job, outputPath);
		//outputPath.getFileSystem(conf).delete(outputPath);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
