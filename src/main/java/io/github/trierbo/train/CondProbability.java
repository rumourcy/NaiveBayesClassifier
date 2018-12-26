package io.github.trierbo.train;

import io.github.trierbo.NaiveBayes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 用于计算条件概率, 即P(A|C)
 */
public class CondProbability {

    public static class CondProbabilityMapper extends Mapper<Text, Text, Text, DoubleWritable> {
        // 利用KeyValueTextInputFormat读取数据, 其中key是word_country, value是出现的次数
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // 获取country
            String[] splits = key.toString().split("_");
            String country = splits[splits.length - 1];

            // 获取出现次数
            int num = Integer.parseInt(value.toString());
            double prob = Math.log((double) (num + 1) / (NaiveBayes.wordPerCountry.get(country) + NaiveBayes.wordDict));
            NaiveBayes.condProb.put(key.toString(), prob);
            context.write(key, new DoubleWritable(prob));
        }
    }
}
