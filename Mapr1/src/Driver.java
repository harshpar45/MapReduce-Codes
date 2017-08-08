import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class Driver extends Configured implements Tool{

	public static class MapClass extends Mapper<LongWritable,Text, Text, Text> 
	{
        public void map (LongWritable key, Text value, Context context)
        {
        try
        {
        	String[] str=value.toString().split("\\|");
        	String year=str[7].trim().toString();
        	String s_no=str[0].trim().toString();
        	String job_title=str[4].trim().toString();
        	String mykey = year +","+ job_title;
        	context.write(new Text(mykey), new Text(s_no));
        }
        catch(Exception e)
        {
        	System.out.println(e.getMessage());
        }
	}
	}
	public static void main(String[] ar)throws Exception {
		ToolRunner.run(new Configuration(),new Driver(),ar);
		System.exit(0);

		}
	public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
	{
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
		private Text outputKey = new Text();
		private Text result=new Text();
		
		public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
		{
			
		int count=0;
		//String j_title= "";
		for (@SuppressWarnings("unused") Text val : values)
		{		
			++count;			
		}
		
		String myvalue= String.valueOf(key)+","+ String.valueOf(count) ;
		String mykey = String.valueOf(count);
		
		outputKey.set(mykey);
		result.set(myvalue);
		//context.write(outputKey, result);	
		repToRecordMap.put(count, new Text(result));
		if (repToRecordMap.size() > 10) {
					repToRecordMap.remove(repToRecordMap.firstKey());
			}	
		
	}
		protected void cleanup(Context context) throws IOException,
		InterruptedException {
		// Output our 5 records to the reducers with a null key
		for (Text t : repToRecordMap.descendingMap().values()) {
		context.write(NullWritable.get(), t);
			}
		}
		
	}
	public static class CaderPartitioner extends Partitioner <Text,Text>
	{

		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] str = key.toString().split(",");
			if(str[0].trim().equals("2011"))
			return 0;
			else
				if(str[0].trim().equals("2012"))
					return 1;
				else
					if(str[0].trim().equals("2013"))
						return 2;
					else
						if(str[0].trim().equals("2014"))
							return 3;
						else
							if(str[0].trim().equals("2015"))
								return 4;
							else
								if(str[0].trim().equals("2016"))
									return 5;
								else
									return 6;							
			
			}
			}

	@Override
	public int run(String[] arg) throws Exception {
		Configuration conf = new Configuration();
		Job job=Job.getInstance(conf);
		job.setJarByClass(Driver.class);
		job.setJobName("Qu 5 Top 10 job positions for each year");
		FileInputFormat.setInputPaths(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(CaderPartitioner.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(7);
		//job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
	//	job.setOutputKeyClass(NullWritable.class);
	//	job.setOutputValueClass(Text.class);
		System.exit(job.waitForCompletion(true)? 0:1);
		return 0;
		}

}
