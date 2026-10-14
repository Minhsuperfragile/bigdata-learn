package com.example;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeteoHadoop {
    public static class DataMapper
            extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private IntWritable outKey = new IntWritable();
        private IntWritable outValue = new IntWritable();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(":");
            int year = Integer.parseInt(tokens[2].trim());
            int temperature = Integer.parseInt(tokens[3].trim());
            outKey.set(year);
            outValue.set(temperature);
            context.write(outKey, outValue);
        }
    }

    public static class DataReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            for (IntWritable val : values) {
                if (val.get() > max) {
                    max = val.get();
                }
            }
            context.write(key, new IntWritable(max));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "maxTempYear");

    }
}
