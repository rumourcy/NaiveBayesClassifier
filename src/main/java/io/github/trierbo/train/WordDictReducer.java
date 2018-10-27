package io.github.trierbo.train;

import io.github.trierbo.utils.CacheURL;
import io.github.trierbo.utils.TextPairs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class WordDictReducer extends Reducer<Text, NullWritable, TextPairs, IntWritable> {

    private List<String> conutries = new ArrayList<>();

    protected void setup(Context context) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(CacheURL.COUNTRY_URL), context.getConfiguration());
        try (InputStream in = fs.open(new Path(CacheURL.COUNTRY_URL));
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            String pair[];
            while ((line = reader.readLine()) != null) {
                pair = line.split("\t");
                conutries.add(pair[0]);
            }
        }
    }

    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        IntWritable zero = new IntWritable(0);
        TextPairs textPair;
        for (String country: conutries) {
            textPair = new TextPairs(key, new Text(country));
            context.write(textPair, zero);
        }
    }
}
