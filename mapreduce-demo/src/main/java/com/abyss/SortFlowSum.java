package com.abyss;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Abyss on 2018/8/8.
 * description:
 */
//需求：统计出每个手机号，产生的总上行流量、总下行流量、总流量是多少
public class SortFlowSum {

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Flowbean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Flowbean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setJarByClass(SortFlowSum.class);

        FileInputFormat.setInputPaths(job, new Path("/Users/abyss/Dev/toys/flowsum/output"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/abyss/Dev/toys/flowsum/sortoutput"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }

    public static class MyMapper extends Mapper<LongWritable, Text, Flowbean, NullWritable> {
        Flowbean flowbean = new Flowbean();
        NullWritable v = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] arr = value.toString().split("\\s+");
            //设置手机号为key
            flowbean.setPhone(arr[0]);
            //设置上下行流量为value
            flowbean.setUpflow(Long.parseLong(arr[1]));
            flowbean.setDownflow(Long.parseLong(arr[2]));
            flowbean.setSumflow(Long.parseLong(arr[3]));
            context.write(flowbean, v);
        }
    }

    public static class MyReducer extends Reducer<Flowbean, NullWritable, Flowbean, NullWritable> {
        @Override
        protected void reduce(Flowbean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    //序列化
    private static class Flowbean implements WritableComparable<Flowbean> {
        private String phone;
        private long upflow;
        private long downflow;
        private long sumflow;

        @Override
        public String toString() {
            return phone + "\t" + upflow + "\t" + downflow + "\t" + sumflow;
        }

        /**
         * 1、按照总流量从大到小的顺序排序
         * 2、总流量一样，那就按照手机号的自然顺序来排序
         * 13480253104	180	180	360
         * 13502468823	7335	110349	117684
         */
        @Override
        public int compareTo(Flowbean o) {
            //后来的大于前面的
            if (this.sumflow > o.sumflow) {
                return -1;
            }
            if (this.sumflow < o.sumflow) {
                return 1;
            }
            //相同
            int num = this.phone.compareTo(o.phone);
            if (num > 0) {
                return -1;
            }
            if (num < 0) {
                return 1;
            }
            return 0;

        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(phone);
            dataOutput.writeLong(upflow);
            dataOutput.writeLong(downflow);
            dataOutput.writeLong(sumflow);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.phone = dataInput.readUTF();
            this.upflow = dataInput.readLong();
            this.downflow = dataInput.readLong();
            this.sumflow = dataInput.readLong();
        }

        public Flowbean() {
        }

        public Flowbean(String phone, long upflow, long downflow, long sumflow) {
            this.phone = phone;
            this.upflow = upflow;
            this.downflow = downflow;
            this.sumflow = sumflow;
        }

        public String getPhone() {
            return phone;
        }

        public void setPhone(String phone) {
            this.phone = phone;
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

        public long getSumflow() {
            return sumflow;
        }

        public void setSumflow(long sumflow) {
            this.sumflow = sumflow;
        }


    }


}
