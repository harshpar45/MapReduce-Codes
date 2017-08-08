import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
    
	
	private Map<String, String> abMap = new HashMap<String, String>();
	//private Map<String, String> abMap1 = new HashMap<String, String>();
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	protected void setup(Context context) throws java.io.IOException, InterruptedException{
		
		super.setup(context);

	    URI[] files = context.getCacheFiles(); // getCacheFiles returns null

	    Path p = new Path(files[0]);
	
	   // Path p1 = new Path(files[1]);
	
	/*	if (p.getName().equals("POS1.txt")) {
				BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
				String line = reader.readLine();
				while(line != null) {
					String[] tokens = line.split(",");
					String store_id = tokens[0];
					String item_id = tokens[1];
					String qty_sold = tokens[2];
					String punit = tokens[3];
					String store_value = "";
					abMap.put(store_id, store_value);
					line = reader.readLine();
				}
				reader.close();
			}*/
		if (p.getName().equals("store_master-")) {
			BufferedReader reader = new BufferedReader(new FileReader(p.toString()));
			String line = reader.readLine();
			while(line != null) {
				String[] tokens = line.split(",");
				String store_id = tokens[0];
				String state = tokens[2];
				abMap.put(store_id, state);
				line = reader.readLine();
			}
			reader.close();
		}
	
		
		if (abMap.isEmpty()) {
			throw new IOException("MyError:Unable to load POS Data");
		}

	/*	if (abMap1.isEmpty()) {
			throw new IOException("MyError:Unable to load store_master");
		}*/

	}

	
    protected void map(LongWritable key, Text value, Context context)
        throws java.io.IOException, InterruptedException {
    	
    	
    	String row = value.toString();
    	String[] tokens = row.split(",");
    	String store_id = tokens[0];
    	String POS_dat = abMap.get(store_id);
    //	String stat = abMap1.get(store_id);
    	//String combData = POS_dat + "," + stat; 
    	outputKey.set(row);
    	outputValue.set(POS_dat);
  	context.write(outputKey,outputValue);
    }  
}