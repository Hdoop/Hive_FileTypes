package advancemr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Prob2Red extends Reducer<Text, IntWritable, Text, IntWritable>
{
	public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
		int _total = 0;
		
		while(values.iterator().hasNext())
		{
			_total +=values.iterator().next().get();
		}
		context.write(key, new IntWritable(_total));
		
		
	}
}
