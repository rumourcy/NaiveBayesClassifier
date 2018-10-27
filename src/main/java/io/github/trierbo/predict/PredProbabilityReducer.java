package io.github.trierbo.predict;

import io.github.trierbo.utils.CacheURL;
import io.github.trierbo.utils.TextPairs;
import io.github.trierbo.utils.TripleText;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class PredProbabilityReducer extends Reducer<TextPairs, Text, TripleText, DoubleWritable> {

    private HashMap<String, HashMap<String, Double>> condPro = new HashMap<>();

    protected void setup(Context context) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(CacheURL.COND_PROBABILITY_URL), context.getConfiguration());
        try (InputStream in = fs.open(new Path(CacheURL.COND_PROBABILITY_URL));
             BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
            String line;
            String pair[];
            HashMap<String, Double> prob;
            while ((line = reader.readLine()) != null) {
                pair = line.split("\t");
                if (!condPro.containsKey(pair[0])) {
                    prob = new HashMap<>();
                    prob.put(pair[1], Double.parseDouble(pair[2]));
                    condPro.put(pair[0], prob);
                } else {
                    prob = condPro.get(pair[0]);
                    prob.put(pair[1], Double.parseDouble(pair[2]));
                }
            }
        }
    }

    protected void reduce(TextPairs key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Double> prob;
        HashMap<String, Double> clazz = new HashMap<>();
        for(Text value: values) {
            prob = condPro.get(value.toString());
            for (String probKey: prob.keySet()) {
                if (!clazz.containsKey(probKey)) {
                    clazz.put(probKey, prob.get(probKey));
                } else {
                    Double clazzProb = clazz.get(probKey);
                    clazzProb += prob.get(probKey);
                    clazz.put(probKey, clazzProb);
                }
            }
        }
        for(String cla: clazz.keySet()) {
            context.write(new TripleText(key.getWord(), key.getCountry(), new Text(cla)), new DoubleWritable(clazz.get(cla)));
        }
    }
}
