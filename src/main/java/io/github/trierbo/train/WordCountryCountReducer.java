package io.github.trierbo.train;

import io.github.trierbo.utils.TextPairs;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class WordCountryCountReducer extends Reducer<TextPairs, IntWritable, TextPairs, IntWritable> {

    private MultipleOutputs<TextPairs, IntWritable> outputs;

    protected void setup(Context context) {
        outputs = new MultipleOutputs<>(context);
    }

    protected void reduce(TextPairs key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values) {
            sum += value.get();
        }
        if (key.getWord().toString().equals("####")) {
            outputs.write("country", key.getCountry(), new IntWritable(sum));
        } else {
            outputs.write("word", key, new IntWritable(sum));
        }
    }
}
