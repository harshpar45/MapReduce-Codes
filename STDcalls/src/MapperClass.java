import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text phoneNum = new Text();
	IntWritable durationInMinutes = new IntWritable();

	public void map(LongWritable key, Text value,Context con)
			throws IOException, InterruptedException {
		try{
		String[] par = value.toString().split(",");
		if (par[4].equals("1")) {
			phoneNum.set(par[0]);
			String EndTime = par[3];
			String StartTime = par[2];
			long duration = toMillis(EndTime) - toMillis(StartTime);
			durationInMinutes.set((int) (duration / (1000 * 60)));
			con.write(phoneNum, durationInMinutes);
		}
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}

	private long toMillis(String date) throws ParseException
	{
		SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dateformat=null;
		dateformat=format.parse(date);
		
		return dateformat.getTime();
		
	}
}
