package advancemr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Prob1Red extends Reducer<Text, IntWritable, Text, IntWritable>
{
	public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
		
		int totalpercent = 0;
		int ctr =0;
		
		while(values.iterator().hasNext())
		{
			ctr++;
			totalpercent +=values.iterator().next().get();
		}
		
		totalpercent=totalpercent/ctr;
		context.write(key, new IntWritable(totalpercent));
	}
}
