package io.github.trierbo;

import io.github.trierbo.predict.CountryPredict;
import io.github.trierbo.train.*;
import io.github.trierbo.utils.FileNameInputFormat;
import io.github.trierbo.utils.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.HashMap;

/**
 * 贝叶斯公式为:
 * P(C|AB) ~ P(A|C)*P(B|C)*P(C)
 */
public class NaiveBayes {
    // 记录每个类别的总词数, key表示国家名称, value表示总词数
    public static HashMap<String, Integer> wordPerCountry = new HashMap<>();
    // 用于拉普拉斯平滑
    public static int wordDict = 0;
    // 训练集中文件总数, 用于计算每个类别的概率
    public static int newsNum = 0;
    // 每个类别下某个单词出现的概率
    public static HashMap<String, Double> condProb = new HashMap<>();
    // 每个类别出现的概率
    public static HashMap<String, Double> countryProb = new HashMap<>();

    /**
     * args[0] 将训练集相同类别下所有文档合并之后的路径
     * args[1] 未作处理的训练集路径, 用于统计每个类别的文档个数, 方便计算类别的先验概率
     * args[2] Job1的输出路径
     * args[3] Job2的输出路径
     * args[4] Job3的输出路径, 以及Job4的输入路径
     * args[5] Job4的输出路径
     * args[6] Job5的输出路径, 以及Job6的输入路径
     * args[7] Job6的输出路径
     * args[8] 测试集的路径
     * args[9] 预测结果的输出路径
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.ifile.readahead", "false");

        // Job1用于统计每个分类出现的总词数
        Job job1 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '1');
        // Job2用于统计训练集中所有不同单词的个数, 用于拉普拉斯平滑
        Job job2 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '2');
        // Job3用于统计每个分类每个词出现的次数
        Job job3 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '3');
        // Job4用于计算已知分类的条件下每个词出现的概率
        Job job4 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '4');
        // Job5用于统计每个分类中的文件个数
        Job job5 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '5');
        // Job6用于计算每个分类的先验概率
        Job job6 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '6');
        // Job7用于预测每个文件的分类
        Job job7 = Job.getInstance(conf, NaiveBayes.class.getSimpleName() + '7');

        job1.setJarByClass(NaiveBayes.class);
        job1.setMapperClass(WordCountPerCountry.WordCountPerCountryMapper.class);
        job1.setReducerClass(WordCountPerCountry.WordCountPerCountryReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPaths(job1, args[0]);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        job2.setJarByClass(NaiveBayes.class);
        job2.setMapperClass(WordDict.WordDictMapper.class);
        job2.setReducerClass(WordDict.WordDictReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPaths(job2, args[0]);
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        job3.setJarByClass(NaiveBayes.class);
        job3.setMapperClass(WordCountryCount.WordCountryCountMapper.class);
        job3.setReducerClass(WordCountryCount.WordCountryCountReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPaths(job3, args[0]);
        FileOutputFormat.setOutputPath(job3, new Path(args[4]));

        job4.setJarByClass(NaiveBayes.class);
        job4.setMapperClass(CondProbability.CondProbabilityMapper.class);
        job4.setNumReduceTasks(0);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job4, new Path(args[4]));
        FileOutputFormat.setOutputPath(job4, new Path(args[5]));

        job5.setJarByClass(CountryCount.class);
        job5.setMapperClass(CountryCount.CountryCountMapper.class);
        job5.setReducerClass(CountryCount.CountryCountReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        //使用自定义的InputFormat
        job5.setInputFormatClass(WholeFileInputFormat.class);
        FileInputFormat.addInputPaths(job5, args[1]);
        FileOutputFormat.setOutputPath(job5, new Path(args[6]));

        job6.setJarByClass(CountryProbability.class);
        job6.setMapperClass(CountryProbability.CountryProbabilityMapper.class);
        job6.setNumReduceTasks(0);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        job6.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPaths(job6, args[6]);
        FileOutputFormat.setOutputPath(job6, new Path(args[7]));

        job7.setJarByClass(CountryPredict.class);
        job7.setMapperClass(CountryPredict.CountryPredictMapper.class);
        job7.setReducerClass(CountryPredict.CountryPredictReducer.class);
        job7.setMapOutputKeyClass(Text.class);
        job7.setMapOutputValueClass(MapWritable.class);
        job7.setOutputKeyClass(Text.class);
        job7.setOutputValueClass(Text.class);
        //使用自定义的InputFormat
        job7.setInputFormatClass(FileNameInputFormat.class);
        FileInputFormat.addInputPaths(job7, args[8]);
        FileOutputFormat.setOutputPath(job7, new Path(args[9]));

        ControlledJob controlledJob1 = new ControlledJob(conf);
        ControlledJob controlledJob2 = new ControlledJob(conf);
        ControlledJob controlledJob3 = new ControlledJob(conf);
        ControlledJob controlledJob4 = new ControlledJob(conf);
        ControlledJob controlledJob5 = new ControlledJob(conf);
        ControlledJob controlledJob6 = new ControlledJob(conf);
        ControlledJob controlledJob7 = new ControlledJob(conf);

        controlledJob1.setJob(job1);
        controlledJob2.setJob(job2);
        controlledJob3.setJob(job3);
        controlledJob4.setJob(job4);
        controlledJob5.setJob(job5);
        controlledJob6.setJob(job6);
        controlledJob7.setJob(job7);

        // Job4依赖Job1,Job2,Job3
        controlledJob4.addDependingJob(controlledJob1);
        controlledJob4.addDependingJob(controlledJob2);
        controlledJob4.addDependingJob(controlledJob3);
        // Job6依赖Job5
        controlledJob6.addDependingJob(controlledJob5);
        // Job7依赖Job4,Job6
        controlledJob7.addDependingJob(controlledJob4);
        controlledJob7.addDependingJob(controlledJob6);

        JobControl jobControl = new JobControl("NaiveBayes");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);
        jobControl.addJob(controlledJob4);
        jobControl.addJob(controlledJob5);
        jobControl.addJob(controlledJob6);
        jobControl.addJob(controlledJob7);

        Thread thread = new Thread(jobControl);
        thread.start();
        while(true){
            if(jobControl.allFinished()){
                System.out.println(jobControl.getSuccessfulJobList());
                jobControl.stop();
                return;
            }
            if(jobControl.getFailedJobList().size() > 0){
                System.out.println(jobControl.getFailedJobList());
                jobControl.stop();
                return;
            }
        }
    }
}
