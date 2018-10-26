package io.github.trierbo.train;

import io.github.trierbo.utils.TextPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class CondProbability {

    static final String COUNTRY_URL = "hdfs:///user/falcon/result/word_country_count/countryCount-r-00000";
    static final String WORD_DICT_URL = "hdfs:///user/falcon/result/word_dict/part-r-00000";

    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordDict <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.ifile.readahead", "false");
        Job job = Job.getInstance(conf);

        job.setJarByClass(CondProbability.class);
        job.setMapperClass(CondProbabilityMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.addCacheFile(URI.create(COUNTRY_URL));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
