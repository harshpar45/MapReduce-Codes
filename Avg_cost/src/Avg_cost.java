import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Avg_cost {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"");
		job.setJarByClass(Avg_cost.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
	
//Mapper class
	public static class MapClass extends Mapper<LongWritable,Text, Text, Text> 
	{
		
        public void map (LongWritable key, Text value, Context context)
        {
        try
        {
        	String[] str=value.toString().split(";");
        	String prodid=str[5];
        	String cost=str[7].trim();
        	String qty=str[6].trim();
        	String myrow=cost+ "," +qty;
        	context.write(new Text(prodid),new Text(myrow));
        }
        catch(Exception e)
        {
        	System.out.println(e.getMessage());
        }
	}
	}
	//reducer class
			public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
			{
				   private TreeMap<Double, Text> repToRecordMap = new TreeMap<Double, Text>();

				//private Text outputKey = new Text();
				//private IntWritable result=new IntWritable();
				
				public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
			{
					long totalsales=0;
					int totalqty=0;
					double avgcost=0.00; 
					for (Text val : values)
					{
						String[] str=val.toString().split(",");
						totalqty = totalqty+Integer.parseInt(str[1]);
						totalsales = totalsales+Integer.parseInt(str[0]);
					}
					if(totalqty!=0)
					{	
					avgcost=(double)(totalsales/totalqty);
					String myValue=key.toString();
					String myAvgCost=String.format("%f", avgcost);
					myValue=myValue + "," + myAvgCost;
						repToRecordMap.put(new Double(myAvgCost), new Text(myValue));
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

}
