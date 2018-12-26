package io.github.trierbo.train;

import io.github.trierbo.NaiveBayes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 计算每个类别的概率
 */
public class CountryProbability {
    public static class CountryProbabilityMapper extends Mapper<Text, Text, Text, DoubleWritable> {
        // 利用KeyValueTextInputFormat读取数据, 其中key是country, value是出现的次数
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int num = Integer.parseInt(value.toString());
            double prob = Math.log((double) num / NaiveBayes.newsNum);
            NaiveBayes.countryProb.put(key.toString(), prob);
            context.write(key, new DoubleWritable(prob));
        }
    }
}
