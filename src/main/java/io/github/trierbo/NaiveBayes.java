package io.github.trierbo;

import io.github.trierbo.train.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.HashMap;

public class NaiveBayes {
    // 记录每个类别的总词数, key表示国家名称, value表示总词数
    public static HashMap<String, Integer> wordPerCountry = new HashMap<>();
    // 用于拉普拉斯平滑
    public static int wordDict = 0;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.ifile.readahead", "false");

        Job job1 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '1');
        Job job2 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '2');
        Job job3 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '3');
        Job job4 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '4');

        job1.setJarByClass(NaiveBayes.class);
        job1.setMapperClass(WordCountPerCountry.WordCountPerCountryMapper.class);
        job1.setReducerClass(WordCountPerCountry.WordCountPerCountryReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPaths(job1, args[0]);
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job2.setJarByClass(NaiveBayes.class);
        job2.setMapperClass(WordDict.WordDictMapper.class);
        job2.setReducerClass(WordDict.WordDictReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPaths(job2, args[0]);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job3.setJarByClass(NaiveBayes.class);
        job3.setMapperClass(WordCountryCount.WordCountryCountMapper.class);
        job3.setReducerClass(WordCountryCount.WordCountryCountReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPaths(job3, args[0]);
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        job4.setJarByClass(NaiveBayes.class);
        job4.setMapperClass(CondProbability.CondProbabilityMapper.class);
        job4.setNumReduceTasks(0);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job4, new Path(args[3]));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));

        ControlledJob controlledJob1 = new ControlledJob(conf);
        ControlledJob controlledJob2 = new ControlledJob(conf);
        ControlledJob controlledJob3 = new ControlledJob(conf);
        ControlledJob controlledJob4 = new ControlledJob(conf);

        controlledJob1.setJob(job1);
        controlledJob2.setJob(job2);
        controlledJob3.setJob(job3);
        controlledJob4.setJob(job4);

        controlledJob4.addDependingJob(controlledJob1);
        controlledJob4.addDependingJob(controlledJob2);
        controlledJob4.addDependingJob(controlledJob3);

        JobControl jobControl = new JobControl("NaiveBayes");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);
        jobControl.addJob(controlledJob4);

        Thread thread = new Thread(jobControl);
        thread.start();
        while(true){
            if(jobControl.allFinished()){
                System.out.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                return;
            }
            if(jobControl.getFailedJobList().size() > 0){
                System.out.println(jobControl.getFailedJobList());
                jobControl.stop();
                return;
            }
        }
    }
}
