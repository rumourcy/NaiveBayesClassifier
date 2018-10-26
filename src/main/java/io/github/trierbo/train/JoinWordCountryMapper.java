package io.github.trierbo.train;

import io.github.trierbo.utils.CacheURL;
import io.github.trierbo.utils.TextPair;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class JoinWordCountryMapper extends Mapper<LongWritable, Text, TextPair, IntWritable> {

    private HashMap<TextPair, Integer> wordCountryMaps = new HashMap<>();

    protected void setup(Context context) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(CacheURL.WORD_COUNTRY_URL), context.getConfiguration());
        try (InputStream in = fs.open(new Path(CacheURL.WORD_COUNTRY_URL));
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            String pair[];
            while ((line = reader.readLine()) != null) {
                pair = line.split("\t");
                wordCountryMaps.put(new TextPair(pair[0], pair[1]), Integer.parseInt(pair[2]));
            }
        }
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String pair[] = line.split("\t");
        TextPair textPair = new TextPair(pair[0], pair[1]);
        IntWritable count = new IntWritable(Integer.parseInt(pair[2]));
        if (wordCountryMaps.containsKey(textPair)) {
            count.set(wordCountryMaps.get(textPair));
        }
        context.write(textPair, count);
    }
}
