import java.io.IOException;

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



public class NQ2Driver {
	
	public static class MapClass extends Mapper<LongWritable,Text,NullWritable,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {
	         try{
	            String[] str = value.toString().split(";");
	            //String custid = str[1].trim();
	           // String prodid = str[5];
	            String sales = str[8].trim();
	            String qty = str[6].trim();
	           // long amount = Long.parseLong(str[8]);
	           // String onee = "1";
	           // public static Text one = new Text(onee.toString());
	            String row = sales + ","+ qty;
	            context.write(NullWritable.get(), new Text(row));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
		public static class ReducerClass extends Reducer<NullWritable, Text, NullWritable, Text> {
		
		static int result=0,sum=0,qty=0; 
		public void reduce(NullWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			for(Text val : values)
			{
				String[] str = val.toString().split(",");
				sum+= Integer.parseInt(str[0]);
				qty+=Integer.parseInt(str[1]);
				
			}
			
			String sums = String.valueOf(sum);
			String qtys = String.valueOf(qty);
			String myKey= sums + "," + qtys;
			//String sum1,count1;
			//sum1 = String.valueOf(sum);
			//count1 = String.valueOf(count);
			//String result0 =sum1+","+ count1;
			context.write(key, new Text(myKey));
		}
	
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
  		Job job = Job.getInstance(conf, "Top 5 Records");
  	    job.setJarByClass(NQ2Driver.class);
  	    job.setMapperClass(MapClass.class);
  	    job.setReducerClass(ReducerClass.class);
  	    job.setMapOutputKeyClass(NullWritable.class);
  	    job.setMapOutputValueClass(Text.class);
  	    job.setOutputKeyClass(NullWritable.class);
  	    job.setOutputValueClass(Text.class);
  	    FileInputFormat.addInputPath(job, new Path(args[0]));
  	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
  	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
