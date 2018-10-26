package io.github.trierbo.train;

import io.github.trierbo.utils.TextPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WordClassCountMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        String fileName = ((FileSplit) inputSplit).getPath().toString();
        TextPair textPair = new TextPair(value, new Text(fileName));
        IntWritable one = new IntWritable(1);
        context.write(textPair, one);
    }
}
