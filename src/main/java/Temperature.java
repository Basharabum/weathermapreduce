import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Temperature {
    public static void main(String[] args) throws Exception {
        JobControl jobControl = new JobControl("jobChain");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Temperature.class);
        job.setJobName("Max temperature");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job);

        jobControl.addJob(controlledJob1);

        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(Temperature.class);
        job2.setJobName("Max temperature 2 years");

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "total"));

        job2.setMapperClass(TemperatureMapperTotal.class);
        job2.setReducerClass(TemperatureReducerTotal.class);

        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputValueClass(Text.class);

        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);

        jobControl.addJob(controlledJob2);

        controlledJob2.addDependingJob(controlledJob1);



        jobControl.run();

        System.exit(job.waitForCompletion(true) ? 0 : 1);


        /*Thread jobRunnerThread = new Thread(new JobRunner(jobctrl));
        jobRunnerThread.start();

        while (!jobctrl.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(5000);
        }
        System.out.println("done");
        jobctrl.stop();
        System.exit(job2.waitForCompletion(true) ? 0 : 1);*/
    }
}

/*class JobRunner implements Runnable {
	private JobControl control;

	public JobRunner(JobControl _control) {
		this.control = _control;
	}

	public void run() {
		this.control.run();
	}
}*/
