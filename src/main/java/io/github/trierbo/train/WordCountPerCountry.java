package io.github.trierbo.train;

import io.github.trierbo.NaiveBayes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 统计每个类别下文档的单词总数, 用于计算P(A|C)
 * 在未进行拉普拉斯平滑时:
 * P(A|C) = 该类别下A出现的次数/该类别下文档的词数
 */
public class WordCountPerCountry {
    public static class WordCountPerCountryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            // 通过文件路径获取类别
            String path = ((FileSplit) inputSplit).getPath().toString();
            String temp[] = path.split("/");
            String country = temp[temp.length - 1];
            IntWritable one = new IntWritable(1);
            context.write(new Text(country), one);
        }
    }

    public static class WordCountPerCountryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            // 将统计结果作为中间结果存放在内存中
            NaiveBayes.wordPerCountry.put(key.toString(), sum);
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

        job.setJarByClass(WordCountryCount.class);
        job.setMapperClass(WordCountPerCountryMapper.class);
        job.setReducerClass(WordCountPerCountryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
