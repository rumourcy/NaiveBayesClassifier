package io.github.trierbo;

import io.github.trierbo.train.*;
import io.github.trierbo.utils.CacheURL;
import io.github.trierbo.utils.TextPairs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class NaiveBayesTrain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.ifile.readahead", "false");

        Job job1 = Job.getInstance(conf, NaiveBayesTrain.class.getSimpleName() + '1');
        Job job2 = Job.getInstance(conf, NaiveBayesTrain.class.getSimpleName() + '2');
        Job job3 = Job.getInstance(conf, NaiveBayesTrain.class.getSimpleName() + '3');

        job1.setJarByClass(NaiveBayesTrain.class);
        job1.setMapperClass(WordDictMapper.class);
        job1.setReducerClass(WordDictReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(NullWritable.class);
        job1.setOutputKeyClass(TextPairs.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.addCacheFile(URI.create(CacheURL.COUNTRY_URL));
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job2.setJarByClass(NaiveBayesTrain.class);
        job2.setMapperClass(JoinWordCountryMapper.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(TextPairs.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.addCacheFile(URI.create(CacheURL.WORD_COUNTRY_URL));
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job3.setJarByClass(NaiveBayesTrain.class);
        job3.setMapperClass(CondProbabilityMapper.class);
        job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(TextPairs.class);
        job3.setOutputValueClass(DoubleWritable.class);
        job3.addCacheFile(URI.create(CacheURL.COUNTRY_URL));
        job3.addCacheFile(URI.create(CacheURL.WORD_DICT_URL));
        FileInputFormat.addInputPath(job3, new Path(args[2]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        ControlledJob controlledJob1 = new ControlledJob(conf);
        ControlledJob controlledJob2 = new ControlledJob(conf);
        ControlledJob controlledJob3 = new ControlledJob(conf);

        controlledJob1.setJob(job1);
        controlledJob2.setJob(job2);
        controlledJob3.setJob(job3);

        controlledJob2.addDependingJob(controlledJob1);
        controlledJob3.addDependingJob(controlledJob2);

        JobControl jobControl = new JobControl("NaiveBayesTrain");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);

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
