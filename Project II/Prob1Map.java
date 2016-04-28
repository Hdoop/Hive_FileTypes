package advancemr;

import java.io.IOException;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;


public class Prob1Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
	public void map (LongWritable key ,Text value,Context context) throws IOException, InterruptedException
	{
		IntWritable percent =new IntWritable();
		Text compkey =new Text();
		if(value != null)
		{
			String []values = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
			
			//String temp="H:"+values[0].toString()+" Q:"+values[1].toString();
			String temp=values[0].toString();
			compkey.set(temp);
			percent.set(Integer.parseInt(values[3].toString()));
		}
		context.write(compkey, percent);
	}
}

