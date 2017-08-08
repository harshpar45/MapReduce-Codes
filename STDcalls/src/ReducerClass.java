import java.io.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
	IntWritable result=new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> value,Context con) throws IOException,InterruptedException
	{
		long sum=0;
		for(IntWritable val:value)
		{
			sum+=val.get();
		}
		result.set((int)sum);
		if(sum>=60)
		{
			con.write(key, result);
		}
	}

}
