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

public class Driver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf,"Margin%");
	job.setJarByClass(Driver.class);
	job.setMapperClass(MyMapper.class);
	job.setReducerClass(MyReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true)?0:1);

	}

	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
		{
			try{
				String[] str = value.toString().split(";");
				String prodid=str[5];
				String sales = str[8];
				String cost = str[7];
				String qty=str[6];
				String myValue = qty+','+cost+','+sales;
				context.write(new Text(prodid), new Text(myValue));
			}
			catch(Exception ex)
			{
				System.out.println(ex.getMessage());
			}
		}
	}
	
	public static class MyReducer extends Reducer<Text,Text,NullWritable,Text>
	{
		String myValue;
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		{
			long totalSales = 0,totalCost = 0, profit=0;int totalQty = 0;double margin = 0.00;
			for(Text val : values)
			{
				String[] token = val.toString().split(",");
				totalSales = totalSales+Long.parseLong(token[2]);
				totalCost = totalCost+Long.parseLong(token[1]);
				totalQty = totalQty+Integer.parseInt(token[0]);
			}
			profit = totalSales-totalCost;
			if(totalCost !=0)
			{
				margin = ((totalSales-totalCost)*100)/totalCost;
				
			}else
				margin=((totalSales-totalCost)*100);
			
			myValue = key.toString();
			String myProfit = String.valueOf(profit);
			String myMargin = String.valueOf(margin);
			String myQty = String.valueOf(totalQty);
			myValue = myValue+','+ myQty+','+ myProfit+','+myMargin;
			context.write(NullWritable.get(), new Text(myValue));
		}
		//protected void cleanup(Context context) throws IOException,InterruptedException{
			
		//}
	}
}

