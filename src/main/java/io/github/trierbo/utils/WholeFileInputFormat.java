package io.github.trierbo.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * key: Text 内容为类别名
 * value: IntWritable 用于计数
 */
public class WholeFileInputFormat extends FileInputFormat<Text, IntWritable> {

    // 文档不可分, 使得针对每个文档产生一个类别
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, IntWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WholeFileRecordReader recordReader = new WholeFileRecordReader();
        recordReader.initialize(inputSplit, taskAttemptContext);
        return recordReader;
    }
}
