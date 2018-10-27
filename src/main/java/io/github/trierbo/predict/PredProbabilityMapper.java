package io.github.trierbo.predict;

import io.github.trierbo.utils.TextPairs;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class PredProbabilityMapper extends Mapper<LongWritable, Text, TextPairs, Text> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();
        String path = ((FileSplit) inputSplit).getPath().toString();
        String temp[] = path.split("/");
        String country = temp[temp.length - 2];
        TextPairs outputKey = new TextPairs(path, country);
        context.write(outputKey, value);
    }
}
