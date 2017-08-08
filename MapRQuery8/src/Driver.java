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
        	String year = str[7].trim().replace("\\N","2017").toString();
        	String pre_wage = str[6].trim().replace("\\N","0").toString();
        	String job_title=str[4].trim().replace("\\N","null").toString();
        	String fp_time = str[5].trim().replace("\\N","NA").toString();
        	String mykey = year+","+job_title;
        	String myvalue = pre_wage+","+fp_time;
        	context.write(new Text(mykey), new Text(myvalue));
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
		//private Text outputKey = new Text();
		//private Text result=new Text();
		
		public void reduce(Text key,Iterable <Text> values, Context context) throws IOException, InterruptedException
		{
			try{
			int sumy=0,county=0,avgy=0,sumn=0,countn=0,avgn=0;
			for (Text t : values) 
			{
				String parts[] = t.toString().split(",");
				if(parts[1].equals("Y"))
				{
					++county;
					sumy+=Integer.parseInt(String.valueOf(parts[0]));
					avgy=sumy/county;
					
				}else if(parts[1].equals("N"))
				{
					++countn;
					sumn+=Integer.parseInt(String.valueOf(parts[0]));
					avgn=sumn/countn;
				}
				}
			String myvalue = key+","+avgy+","+avgn;
			repToRecordMap.put(avgy, new Text(myvalue));
			}
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
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
		job.setJobName("Qu 8 Avg prevailaing wage for each job each year");
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
