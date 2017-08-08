import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SentimentPercent {
public static class TokenizerMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
        
		
		private Map<String, String> abMap = new HashMap<String, String>();
		private static IntWritable total_value=new IntWritable();
		private Text word=new Text();
		String myword ="";
		int myvalue=0;
		
		protected void setup(Context context) throws java.io.IOException, InterruptedException{
			
			super.setup(context);

		    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

		    Path p = new Path(files[0]);
		
			if (p.getName().equals("AFINN.txt")) {
					BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
					String line = reader.readLine();
					while(line != null) {
						String[] tokens = line.split("\t");
						String diction_word = tokens[0];
						String diction_value = tokens[1];
						abMap.put(diction_word, diction_value);
						line = reader.readLine();
					}
					reader.close();
				}
			
			if (abMap.isEmpty()) {
				throw new IOException("MyError:Unable to load data.");
			}

		}
protected void map(LongWritable key, Text value, Context context)
        throws java.io.IOException, InterruptedException {
    	
 StringTokenizer itr= new StringTokenizer(value.toString());
 while(itr.hasMoreTokens())
 {
	myword = itr.nextToken().toLowerCase();
	 
	 if (abMap.get(myword)!=null)
	 {
		myvalue=Integer.parseInt(abMap.get(myword)); 
		if (myvalue>0)
		{
			myword="positive";
		}
		if (myvalue<0)
		{
			myword="negative";
			myvalue= myvalue * -1;
		}
	 }
	 else
	 {
		 myword= "positive";
		 myvalue=0;
	 }
	 word.set(myword);
	 total_value.set(myvalue);
	 context.write(word, total_value);
 }
}
} 
public static class TokenizerReducer extends
Reducer<Text, IntWritable, NullWritable, Text> {
    int pos_total=0;
    int neg_total=0;
    double sentpercent=0.00;
	public void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException
	{
	int sum=0;
	
	for(IntWritable val : values)
	{
	sum += val.get();	
	}
	if(key.toString().equals("positive"))
	{
	pos_total=sum;	
	}
	if(key.toString().equals("negative"))
	{
	neg_total=sum;	
	}
	}
protected void cleanup(Context context) throws IOException,InterruptedException
{
	sentpercent=(((double)pos_total)-((double)neg_total)/((double)pos_total)+((double)neg_total));
	String str ="Sentiment percent for the given text is: "+ String.format("%f",sentpercent);
	context.write(NullWritable.get(), new Text(str));
}
}
public static void main(String[] args) 
        throws IOException, ClassNotFoundException, InterruptedException {

Configuration conf = new Configuration();
Job job = Job.getInstance(conf,"sentiment %");
job.setJarByClass(SentimentPercent.class);
job.setJobName("Sentiment Analysis");
job.setMapperClass(TokenizerMapper.class);
job.setReducerClass(TokenizerReducer.class);
job.addCacheFile(new Path("AFINN.txt").toUri());
job.setNumReduceTasks(1);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(Text.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.waitForCompletion(true);


}

}


