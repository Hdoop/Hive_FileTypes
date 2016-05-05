package mr.lib.code;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Mr1Driver {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "Advance MR Demo");
	
	    job.setJarByClass(Mr1Driver.class);
	
	    job.setMapperClass(MrMap.class);
	    job.setReducerClass(Red.class);
	
	    job.setOutputKeyClass(Text.class);
	
	    job.setOutputValueClass(DoubleWritable.class);
	    job.setSortComparatorClass(DoubleComparator.class );
	    //job.setSortComparatorClass(LongWritable.DecreasingComparator.class );
		
	    try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));	
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
