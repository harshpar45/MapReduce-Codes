import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class NQ3ADriver extends Configured implements Tool{

	public static void main(String[] ar) throws Exception
	{
		ToolRunner.run(new Configuration(), new NQ3ADriver(),ar);
		System.exit(0);	
		
	}
	public static class MapClass extends Mapper<LongWritable,Text, Text, Text> 
	{
        public void map (LongWritable key, Text value, Context context)
        {
        try
        {
        	String[] str=value.toString().split(";");
        	String prodid=str[5].trim();
        	String sales=str[8].trim();
        	String age=str[2].trim();
        	String cost= str[7].trim();
        	int profit = Integer.parseInt(sales)-Integer.parseInt(cost);
        	String myrow=String.valueOf(profit)+","+age;
        	context.write(new Text(prodid),new Text(myrow));
        }
        catch(Exception e)
        {
        	System.out.println(e.getMessage());
        }
	}
	}
	
	public static class CaderPartitioner extends Partitioner <Text,Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) 
		{
			String[] str = value.toString().split(",");
			String myAge = str[1].trim();
			if(myAge.equals("A"))
				return 0;
			if(myAge.equals("B"))
				return 1;
			if(myAge.equals("C"))
				return 2;
			if(myAge.equals("D"))
				return 3;
			if(myAge.equals("E"))
				return 4;
			if(myAge.equals("F"))
				return 5;
			if(myAge.equals("G"))
				return 6;
			if(myAge.equals("H"))
				return 7;
			if(myAge.equals("I"))
				return 8;
			if(myAge.equals("J"))
				return 9;
			return 10;
			
			}	
	}
	public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
	{
		   private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

		//private Text outputKey = new Text();
		//private IntWritable result=new IntWritable();
		
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	{
			int sum=0;
			String myAge= "";
			for (Text val : values)
			{
			
				String[] str=val.toString().split(",");
				sum += Integer.parseInt(str[0]);
				myAge= str[1];
			}
			String mykey= myAge + "," + key.toString();
			//outputKey.set(mykey);
			//result.set(sum);
			//context.write(outputKey, result);	
			repToRecordMap.put(new Long(sum), new Text(mykey));
			
			if (repToRecordMap.size() > 5) 
				{
						repToRecordMap.remove(repToRecordMap.firstKey());
				}
	      }
		protected void cleanup(Context context) throws IOException,
		InterruptedException 
		{
		
			for (Text t : repToRecordMap.values()) 
			{
					context.write(NullWritable.get(), t);
			}
}
	}

	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(NQ3ADriver.class);
		job.setJobName("Top 5 grossing product age wise");
		FileInputFormat.setInputPaths(job, new Path(arg0[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(CaderPartitioner.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(11);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		System.exit(job.waitForCompletion(true)? 0:1);
		return 0;
	}

}
