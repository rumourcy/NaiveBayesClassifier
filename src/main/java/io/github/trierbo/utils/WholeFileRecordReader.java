package io.github.trierbo.utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends RecordReader<Text, IntWritable> {
    private FileSplit fileSplit;
    private Text key;
    private boolean processed = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        this.fileSplit = (FileSplit) inputSplit;
    }

    // 每个文档是一个split, 只会产生一对key/value
    @Override
    public boolean nextKeyValue() {
        if (!processed) {
            String[] splits = fileSplit.getPath().toString().split("/");
            key = new Text(splits[splits.length - 2]);
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public IntWritable getCurrentValue() {
        return new IntWritable(1);
    }

    @Override
    public float getProgress() {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() {}
}
