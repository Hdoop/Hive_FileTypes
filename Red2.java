package mr.lib.code;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Red2 extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
	
	
	public void reduce(Text key,Iterable<DoubleWritable> values,Context context)
	{
		float _total = 0;		
		while(values.iterator().hasNext())
		{
			_total +=values.iterator().next().get();
		}
		try {
			context.write(key, new DoubleWritable(_total));
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
