import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DriverQ10 {

	public static class MapClass extends Mapper<LongWritable,Text, Text, Text> 
	{
        public void map (LongWritable key, Text value, Context context)
        {
        try
        {
        	String[] str = value.toString().split("\\|");
        	String s_no = str[0].trim().replace("\\N","0").toString();
        	String case_status = str[1].trim().replace("\\N","DENIED").toString();
        	String job_title = str[4].trim().replace("\\N","null").toString();
        	job_title= job_title.replace(".", "");
        	//String fp_time = str[5].trim().replace("\\N","NA").toString();
        	String mykey = job_title;
        	String myvalue = s_no+","+case_status;
        	context.write(new Text(mykey), new Text(myvalue));
        }
        catch(Exception e)
        {
        	System.out.println(e.getMessage());
        }
	}
	
}
	public static class ReduceClass extends Reducer<Text,Text,Text,Text>
	   {
		  public TreeMap<Text, Text> repToRecordMap = new TreeMap<Text, Text>();
	//	  public TreeMap<Double, Text> repToRecordMapf = new TreeMap<Double, Text>();
	      public void reduce(Text key, Iterable <Text> values, Context context) throws IOException, InterruptedException
	      {
	        // long sum = 0;
	         String myvalue="";
	         int countT=0, countC=0,countCW=0;
	         for (Text val : values)
	         {
	        	 String parts[] = val.toString().split(",");
	        	 ++countT;
	        	 if(parts[1].equals("CERTIFIED"))
	        		 ++countC;
	        	 if(parts[1].equals("CERTIFIED-WITHDRAWN"))
	        		 ++countCW;	        	
	         }
	         if(countT>1000)
	         {
	        	myvalue= key+";"+countC +";"+ countCW+ ";"+ countT;
	        	//context.write(new Text(key), new Text(myvalue));
	        	repToRecordMap.put(new Text(key), new Text(myvalue));
	         }
	         }
	      protected void cleanup(Context context) throws IOException,
			InterruptedException 
			{
	    	  
			   String myKey="",  cCert="", cwCert ="", countT="", myText="";
				for (Text t : repToRecordMap.values()) 
				{
					String[] token = t.toString().split(";");
					myKey = token[0];cCert=token[1]; cwCert=token[2]; countT=token[3];
					double avgp = (((Integer.parseInt(cCert))+(Integer.parseInt(cwCert)))*100)/Integer.parseInt(countT);
					if(avgp >= 70.0)
					{
						myText=cCert +"\t"+ cwCert+"\t"+countT+"\t"+avgp;
					context.write(new Text(myKey),new Text(myText));
						
					}
				}
	}
	         }
	   
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"");
		job.setJarByClass(DriverQ10.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
	//	job.setOutputKeyClass(Tex.class);
		job.setOutputValueClass(Text.class);
		

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1); 
	}
	
}
