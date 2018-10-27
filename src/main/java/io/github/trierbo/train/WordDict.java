package io.github.trierbo.train;

import io.github.trierbo.utils.CacheURL;
import io.github.trierbo.utils.TextPairs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WordDict {


    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordDict <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.ifile.readahead", "false");
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordDict.class);
        job.setMapperClass(WordDictMapper.class);
        job.setReducerClass(WordDictReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(TextPairs.class);
        job.setOutputValueClass(IntWritable.class);

        job.addCacheFile(URI.create(CacheURL.COUNTRY_URL));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
