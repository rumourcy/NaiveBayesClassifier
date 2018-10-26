package io.github.trierbo.train;

import io.github.trierbo.utils.TextPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class WordCountryCountReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable> {

    private MultipleOutputs<TextPair, IntWritable> outputs;

    protected void setup(Context context) {
        outputs = new MultipleOutputs<>(context);
    }

    protected void reduce(TextPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values) {
            sum += value.get();
        }
        if (key.getWord().toString().equals("####")) {
            outputs.write("countryCount", key.getCountry(), new IntWritable(sum));
        } else {
            outputs.write("wordCountryCount", key, new IntWritable(sum));
        }
    }
}
