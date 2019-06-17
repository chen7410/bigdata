import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		Configuration conf1 = this.getConf();
		//Job 1
	    Job job1 = Job.getInstance(conf1, "word count");
	    job1.setJarByClass(WordCount.class);
	    job1.setMapperClass(WordMapper.class);
	    job1.setCombinerClass(WordReducer.class);
	    job1.setReducerClass(WordReducer.class);
	    
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));
	    job1.waitForCompletion(true);
	    
//	    JobControl jobControl = new JobControl("chain jobs");
//	    ControlledJob controlledJob1 = new ControlledJob(conf1);
//	    controlledJob1.setJob(job1);
//	    jobControl.addJob(controlledJob1);
	    
	    //job 2
	    Configuration conf2 = this.getConf();
	    Job job2 = Job.getInstance(conf2);
	    
	    job2.setJobName("sort by frequency");
	    job2.setJarByClass(WordCount.class);
	    job2.setMapperClass(SortMapper.class);
	    job2.setCombinerClass(SortReducer.class);
	    job2.setReducerClass(SortReducer.class);
	    job2.setSortComparatorClass(CompositeKeyComparator.class);
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    
	    job2.setInputFormatClass(TextInputFormat.class);
	    job2.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));
	    
//	    ControlledJob controlledJob2 = new ControlledJob(conf2);
//	    controlledJob2.setJob(job2);
	    
	    //make job 2 dependents on job1
//	    controlledJob2.addDependingJob(controlledJob1);
//	    jobControl.addJob(controlledJob2);
//	    Thread jobControlThread = new Thread(jobControl);
//	    jobControlThread.start();
//	    
//	    while (!jobControl.allFinished()) {
//	    System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
//	    System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
//	    System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
//	    System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
//	    System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
//	try {
//	    Thread.sleep(5000);
//	    } catch (Exception e) {
//
//	    }
//
//	  } 
//	   System.exit(0);  
	   return (job2.waitForCompletion(true) ? 0 : 1);   

	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run((Tool) new WordCount(), args);  
		  System.exit(exitCode);
	}
}
