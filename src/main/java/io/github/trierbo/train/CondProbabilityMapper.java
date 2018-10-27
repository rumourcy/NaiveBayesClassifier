package io.github.trierbo.train;

import io.github.trierbo.utils.CacheURL;
import io.github.trierbo.utils.TextPairs;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class CondProbabilityMapper extends Mapper<LongWritable, Text, TextPairs, DoubleWritable> {

    private HashMap<String, Integer> countries = new HashMap<>();
    private int sum = 0;

    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create(CacheURL.COUNTRY_URL), context.getConfiguration());
        try (InputStream inCountry = fs.open(new Path(CacheURL.COUNTRY_URL));
             BufferedReader readerCountry = new BufferedReader(new InputStreamReader(inCountry));
             InputStream inWord = fs.open(new Path(CacheURL.WORD_DICT_URL));
             BufferedReader readerWord = new BufferedReader(new InputStreamReader(inWord))) {
            String line;
            String pair[];
            // 记录单词个数总和
            int all = 0;
            while ((line = readerCountry.readLine()) != null) {
                pair = line.split("\t");
                countries.put(pair[0], Integer.parseInt(pair[1]));
            }
            for (String key : countries.keySet()) {
                all += countries.get(key);
            }
            // 计算每个类别的概率
            for (String key : countries.keySet()) {
                double prob = Math.log((double) countries.get(key) / all);
                context.write(new TextPairs("####", key), new DoubleWritable(prob));
            }
            while (readerWord.readLine() != null) {
                ++sum;
            }
        }
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String pair[] = line.split("\t");
        int all = countries.get(pair[1]);
        int num = Integer.parseInt(pair[2]);
        double prob = Math.log((double) (num + 1) / (all + sum));
        context.write(new TextPairs(pair[0], pair[1]), new DoubleWritable(prob));
    }
}
