package com.abyss;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Abyss on 2018/8/8.
 * description:
 */
//需求：统计出每个手机号，产生的总上行流量、总下行流量、总流量是多少
public class FlowSum {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flowbean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flowbean.class);

        job.setJarByClass(FlowSum.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/abyss/Dev/toys/flowsum/input/HTTP_20130313143750.text"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/abyss/Dev/toys/flowsum/output"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Flowbean> {
        Flowbean flowbean = new Flowbean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("\\s+");
            //设置手机号为key
            value.set(arr[1]);
            //设置上下行流量为value
            flowbean.setUpflow(Long.parseLong(arr[arr.length - 3]));
            flowbean.setDownflow(Long.parseLong(arr[arr.length - 2]));
            context.write(value, flowbean);
        }
    }

    public static class MyReducer extends Reducer<Text, Flowbean, Text, Flowbean> {
        //用来保存总流量
        Flowbean v = new Flowbean();

        @Override
        protected void reduce(Text key, Iterable<Flowbean> values, Context context) throws IOException, InterruptedException {
            long upSum = 0;
            long downSum = 0;
            Iterator<Flowbean> iterator = values.iterator();
            while (iterator.hasNext()) {
                Flowbean next = iterator.next();
                upSum += next.getUpflow();
                downSum += next.getDownflow();
            }
            v.setUpflow(upSum);
            v.setDownflow(downSum);
            context.write(key, v);
        }

    }

    //序列化
    private static class Flowbean implements Writable {
        private long upflow;
        private long downflow;

        @Override
        public String toString() {
            return upflow + "\t" + downflow + "\t" + (upflow + downflow);
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(upflow);
            dataOutput.writeLong(downflow);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.upflow = dataInput.readLong();
            this.downflow = dataInput.readLong();
        }

        public Flowbean() {
        }

        public Flowbean(long upflow, long downflow) {
            this.upflow = upflow;
            this.downflow = downflow;
        }

        public long getUpflow() {
            return upflow;
        }

        public void setUpflow(long upflow) {
            this.upflow = upflow;
        }

        public long getDownflow() {
            return downflow;
        }

        public void setDownflow(long downflow) {
            this.downflow = downflow;
        }


    }


}
