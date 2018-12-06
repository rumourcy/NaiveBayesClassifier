package io.github.trierbo.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class FileNameRecordReader extends RecordReader<Text, Text> {

    private LineReader lineReader;
    private Text key;
    private Text value = new Text();
    private long start;
    private long end;
    private long pos;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
        FileSplit split =(FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        Path path = split.getPath();
        this.key = new Text(path.toString());
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream is = fs.open(path);
        lineReader = new LineReader(is,conf);

        // 处理起始点和终止点
        start =split.getStart();
        end = start + split.getLength();
        is.seek(start);
        if(start!=0){
            start += lineReader.readLine(new Text(),0,
                    (int)Math.min(Integer.MAX_VALUE, end-start));
        }
        pos = start;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (pos > end)
            return false;
        pos += lineReader.readLine(value);
        return value.getLength() != 0;
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }

}
