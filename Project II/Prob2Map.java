package advancemr;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class Prob2Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
	public void map (LongWritable key ,Text value,Context context) throws IOException, InterruptedException
	{
		IntWritable percent =new IntWritable();
		Text compkey =new Text();
		if(value != null)
		{
			//String regex =[^,\"']+|\"([^\"]*)\"
			
			String []values = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			
			String temp=values[1].toString();
			compkey.set(temp);
			percent.set(1);
		}
		context.write(compkey, percent);
	}
}

