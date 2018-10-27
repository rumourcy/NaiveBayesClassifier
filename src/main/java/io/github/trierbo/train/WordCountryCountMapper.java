package io.github.trierbo.train;

import io.github.trierbo.utils.TextPairs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WordCountryCountMapper extends Mapper<LongWritable, Text, TextPairs, IntWritable> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        String path = ((FileSplit) inputSplit).getPath().toString();
        String temp[] = path.split("/");
        String country = temp[temp.length - 1];
        TextPairs textPair = new TextPairs(value, new Text(country));
        IntWritable one = new IntWritable(1);
        context.write(textPair, one);
        TextPairs clazz = new TextPairs("####", country);
        context.write(clazz, one);
    }
}
