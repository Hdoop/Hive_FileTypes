package advancemr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Prob1Driver {

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
	    @SuppressWarnings("deprecation")
		Job job = new Job(conf, "Advance MR Demo");
	
	    job.setJarByClass(Prob1Driver.class);
	
	    job.setMapperClass(Prob1Map.class);
	    job.setReducerClass(Prob1Red.class);
	
	    job.setOutputKeyClass(Text.class);
	
	    job.setOutputValueClass(IntWritable.class);
	    job.setSortComparatorClass(LongWritable.DecreasingComparator.class );
		
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
