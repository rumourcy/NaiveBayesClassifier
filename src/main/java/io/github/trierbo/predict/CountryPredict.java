package io.github.trierbo.predict;

import io.github.trierbo.NaiveBayes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class CountryPredict {
    public static class CountryPredictMapper extends Mapper<Text, Text, Text, MapWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // 记录当前value每个类别下的条件概率
            MapWritable wordCondPro = new MapWritable();
            for (HashMap.Entry<String, Double> entry : NaiveBayes.countryProb.entrySet()) {
                // word + country
                String wordCountry = value.toString() + '_' + entry.getKey();
                if (NaiveBayes.condProb.containsKey(wordCountry)) {
                    wordCondPro.put(new Text(entry.getKey()), new DoubleWritable(NaiveBayes.condProb.get(wordCountry)));
                } else {
                    wordCondPro.put(new Text(entry.getKey()),
                            new DoubleWritable(Math.log(
                                    (double) 1 / ((NaiveBayes.wordPerCountry.get(entry.getKey())) + NaiveBayes.wordDict))));
                }
            }
            context.write(key, wordCondPro);
        }
    }

    public static class CountryPredictReducer extends Reducer<Text, MapWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Double> countryProb = new HashMap<>();
            for (MapWritable value: values) {
                for (String country: NaiveBayes.countryProb.keySet()) {
                    if (!countryProb.containsKey(country)) {
                        countryProb.put(country, ((DoubleWritable)value.get(new Text(country))).get());
                    } else {
                        countryProb.put(country, countryProb.get(country) + ((DoubleWritable)value.get(new Text(country))).get());
                    }
                }
            }
            double max = Double.NEGATIVE_INFINITY;
            String country = null;
            for (HashMap.Entry<String, Double> entry: NaiveBayes.countryProb.entrySet()) {
                double prob = countryProb.get(entry.getKey()) + entry.getValue();
                if (prob > max) {
                    max = prob;
                    country = entry.getKey();
                }
            }
            context.write(key, new Text(country));
        }
    }

}
