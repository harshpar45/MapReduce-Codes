import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Driver {
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(";");
	            String custid = str[1].trim();
	           // String prodid = str[5];
	            String sales = str[8].trim();
	           // long amount = Long.parseLong(str[8]);
	           // String onee = "1";
	           // public static Text one = new Text(onee.toString());
	            context.write(new Text(custid), new Text(sales));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
		public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		
		static int result=0; 
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			int sum=0;
			int count =0;
			for(Text val :values)
			{
			String row = val.toString();
	    	//String[] tokens = row.split(",");
			//for(String val:row)
		//	{
				sum+=Integer.parseInt(row);
				++count;
			}
			String sum1,count1;
			sum1 = String.valueOf(sum);
			count1 = String.valueOf(count);
			String result0 =sum1+","+ count1;
			context.write(key, new Text(result0));
		}
	
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
  		Job job = Job.getInstance(conf, "Top 5 Records");
  	    job.setJarByClass(Driver.class);
  	    job.setMapperClass(MapClass.class);
  	    job.setReducerClass(ReducerClass.class);
  	    job.setMapOutputKeyClass(Text.class);
  	    job.setMapOutputValueClass(Text.class);
  	    job.setOutputKeyClass(Text.class);
  	    job.setOutputValueClass(Text.class);
  	    FileInputFormat.addInputPath(job, new Path(args[0]));
  	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
  	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
