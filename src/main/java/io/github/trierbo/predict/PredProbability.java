package io.github.trierbo.predict;

import io.github.trierbo.utils.CacheURL;
import io.github.trierbo.utils.TextPairs;
import io.github.trierbo.utils.TripleText;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class PredProbability {
    public static void main(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PredProbability <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.ifile.readahead", "false");
        Job job = Job.getInstance(conf);

        job.setJarByClass(PredProbability.class);
        job.setMapperClass(PredProbabilityMapper.class);
        job.setReducerClass(PredProbabilityReducer.class);
        job.setMapOutputKeyClass(TextPairs.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(TripleText.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.addCacheFile(URI.create(CacheURL.COND_PROBABILITY_URL));

        FileInputFormat.addInputPaths(job, args[0]);
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
