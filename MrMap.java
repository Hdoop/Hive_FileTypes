package mr.lib.code;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MrMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException 
	{
		DoubleWritable percent =new DoubleWritable();
		Text compkey =new Text();
		if(value != null)
		{
			String []values = value.toString().split(",");
			if(values.length >3)
			{
				String temp=values[0].toString();
				String tmpvalue=values[3].toString().replace('$', '0');
				String tmpout=values[5].toString().replace('$', '0');
				Double tmpv =0D,tmout=1D;
				try
				{
					tmpv=Double.parseDouble(tmpvalue);
					tmout=Double.parseDouble(tmpout);
					tmpv=tmpv*tmout;
					
				}catch(Exception ex)
				{
					
				}
				compkey.set(temp);			
				percent.set(tmout);				
				context.write(compkey, percent);
			}
		}
		
	}

}
