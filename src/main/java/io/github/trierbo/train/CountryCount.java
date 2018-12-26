package io.github.trierbo.train;

import io.github.trierbo.NaiveBayes;
import io.github.trierbo.utils.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计每个类别下的文档总数
 * 用于计算类别的先验概率P(C)
 */
public class CountryCount {

    public static class CountryCountMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            // 记录所有的文档总数
            NaiveBayes.newsNum++;
            context.write(key, value);
        }
    }

    public static class CountryCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCountPerCountry <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.ifile.readahead", "false");
        Job job = Job.getInstance(conf);

        job.setJarByClass(CountryCount.class);
        job.setMapperClass(CountryCount.CountryCountMapper.class);
        job.setReducerClass(CountryCount.CountryCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //使用自定义的InputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
