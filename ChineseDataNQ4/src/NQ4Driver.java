import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class NQ4Driver {
	public static class MapClass extends Mapper<IntWritable, Text, Text,Text>{
		public void map(IntWritable key, Text value, Context context)
		{
			try{
				String[]str = value.toString().split(";");
				String itemid = str[5].trim();
				String qty = str[6];
				String sales =  str[8];
				String cost =  str[7];
				String myrow = sales+","+qty+","+cost;
				context.write(new Text(itemid), new Text(myrow));
			}
			catch(Exception e)
			{
				
			}
		}
	}
	public static class ReduceClass extends Reducer<Text,Text,NullWritable,Text>
	{
		//private double outputKey = new double();
		private IntWritable result = new IntWritable();
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();
		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException{
			int sumsales=0,sumcost=0,sumqty=0,profit=0;
			String myState = "";Double margin = 0.0;
			
			for(Text val : values)
			{
				String[] str = val.toString().split(",");
				sumsales+= Integer.parseInt(str[0]);
			//	sumqty+=Integer.parseInt(str[1]);
				//sumcost+= Integer.parseInt(str[2]);
			//	if(sumcost!=0){
				//	margin =(double)(sumsales-sumcost)/sumcost*100;}
			//	else
				//	{sumcost=0;}				
			//	profit = sumsales-sumcost;
			}
		//	String qty = String.valueOf(sumqty);
	//		String pro = String.valueOf(profit);
		//	String mar = String.valueOf(margin);
		//	String myrow1= key.toString()+ ","+ mar+","+ pro+","+qty;
			//outputKey.set(margin);
			//result.set(myrow1);
		//	context.write(outputKey, result);
			String myrow1 = key.toString()+sumsales;
			repToRecordMap.put(sumsales, new Text(myrow1));
			for (Text t : repToRecordMap.descendingMap().values()) {
				// Output our five records to the file system with a null key
				context.write(NullWritable.get(), t);
				}
		}
		
	}
	public static void main(String[] args) throws Exception {
	  		
	  		Configuration conf = new Configuration();
	  		Job job = Job.getInstance(conf, "Top 5 Records");
	  	    job.setJarByClass(NQ4Driver.class);
	  	    job.setMapperClass(MapClass.class);
	  	    job.setReducerClass(ReduceClass.class);
	  	    job.setMapOutputKeyClass(Text.class);
	  	    job.setMapOutputValueClass(Text.class);
	  	    job.setOutputKeyClass(NullWritable.class);
	  	    job.setOutputValueClass(Text.class);
	  	    FileInputFormat.addInputPath(job, new Path(args[0]));
	  	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  	  }
		   
}

