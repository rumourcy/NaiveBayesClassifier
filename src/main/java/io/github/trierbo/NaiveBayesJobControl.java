package io.github.trierbo;

import io.github.trierbo.predict.PredProbabilityMapper;
import io.github.trierbo.predict.PredProbabilityReducer;
import io.github.trierbo.train.*;
import io.github.trierbo.utils.CacheURL;
import io.github.trierbo.utils.TextPairs;
import io.github.trierbo.utils.TripleText;
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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class NaiveBayesJobControl {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.ifile.readahead", "false");

        Job job1 = Job.getInstance(conf, NaiveBayesJobControl.class.getSimpleName() + '1');
        Job job2 = Job.getInstance(conf, NaiveBayesJobControl.class.getSimpleName() + '2');
        Job job3 = Job.getInstance(conf, NaiveBayesJobControl.class.getSimpleName() + '3');
        Job job4 = Job.getInstance(conf, NaiveBayesJobControl.class.getSimpleName() + '4');
        Job job5 = Job.getInstance(conf, NaiveBayesJobControl.class.getSimpleName() + '5');

        job1.setJarByClass(NaiveBayesJobControl.class);
        job1.setMapperClass(WordCountryCountMapper.class);
        job1.setReducerClass(WordCountryCountReducer.class);
        job1.setOutputKeyClass(TextPairs.class);
        job1.setOutputValueClass(IntWritable.class);
        MultipleOutputs.addNamedOutput(job1, "word", TextOutputFormat.class, TextPairs.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job1, "country", TextOutputFormat.class, Text.class, IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job2.setJarByClass(NaiveBayesJobControl.class);
        job2.setMapperClass(WordDictMapper.class);
        job2.setReducerClass(WordDictReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NullWritable.class);
        job2.setOutputKeyClass(TextPairs.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.addCacheFile(URI.create(CacheURL.COUNTRY_URL));
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        job3.setJarByClass(NaiveBayesJobControl.class);
        job3.setMapperClass(JoinWordCountryMapper.class);
        job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(TextPairs.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.addCacheFile(URI.create(CacheURL.WORD_COUNTRY_URL));
        FileInputFormat.addInputPath(job3, new Path(args[3]));
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));

        job4.setJarByClass(NaiveBayesJobControl.class);
        job4.setMapperClass(CondProbabilityMapper.class);
        job4.setNumReduceTasks(0);
        job4.setOutputKeyClass(TextPairs.class);
        job4.setOutputValueClass(DoubleWritable.class);
        job4.addCacheFile(URI.create(CacheURL.COUNTRY_URL));
        job4.addCacheFile(URI.create(CacheURL.WORD_DICT_URL));
        FileInputFormat.addInputPath(job4, new Path(args[4]));
        FileOutputFormat.setOutputPath(job4, new Path(args[5]));

        job5.setJarByClass(NaiveBayesJobControl.class);
        job5.setMapperClass(PredProbabilityMapper.class);
        job5.setReducerClass(PredProbabilityReducer.class);
        job5.setMapOutputKeyClass(TextPairs.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setOutputKeyClass(TripleText.class);
        job5.setOutputValueClass(DoubleWritable.class);
        job5.addCacheFile(URI.create(CacheURL.COND_PROBABILITY_URL));
        FileInputFormat.addInputPaths(job5, args[6]);
        FileOutputFormat.setOutputPath(job5, new Path(args[7]));

        ControlledJob controlledJob1 = new ControlledJob(conf);
        ControlledJob controlledJob2 = new ControlledJob(conf);
        ControlledJob controlledJob3 = new ControlledJob(conf);
        ControlledJob controlledJob4 = new ControlledJob(conf);
        ControlledJob controlledJob5 = new ControlledJob(conf);
        controlledJob1.setJob(job1);
        controlledJob2.setJob(job2);
        controlledJob3.setJob(job3);
        controlledJob4.setJob(job4);
        controlledJob5.setJob(job5);

        controlledJob2.addDependingJob(controlledJob1);
        controlledJob3.addDependingJob(controlledJob2);
        controlledJob4.addDependingJob(controlledJob3);
        controlledJob5.addDependingJob(controlledJob4);

        JobControl jobControl = new JobControl("NaiveBayesClassifier");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);
        jobControl.addJob(controlledJob4);
        jobControl.addJob(controlledJob5);

        jobControl.run();
    }
}
