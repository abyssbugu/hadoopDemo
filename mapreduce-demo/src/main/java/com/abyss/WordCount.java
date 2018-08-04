package com.abyss;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Abyss on 2018/7/28.
 * description:统计文本中单词出现次数
 */
public class WordCount {
    private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        IntWritable v = new IntWritable(1);

        /*
        读到一行内容时，就把这一行内容的起始偏移量，赋值给key
        把这一行内容，赋值给value
        context:把map的结果写出去
        */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //拿到一行文本,做切割
            String[] words = value.toString().split("\\s+");
            for (String word : words) {
                value.set(word);
                //key,value
                context.write(value, v);
            }
        }
    }

    private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //对每一行的统计结果,进行归并
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()) {
                count += iterator.next().get();
            }
            v.set(count);
            context.write(key,v);

        }
    }

    public static void main(String[] args) throws Exception {
        //把mapper任务和reducer任务，拼接起来
        Job job = Job.getInstance();    //获取Job对象，我们会把一堆的配置，都配置到job对象中
        job.setJarByClass(WordCount.class);

        job.setMapperClass(MyMapper.class);    //指定任务的mapper阶段，采用哪个类
        job.setReducerClass(MyReducer.class);    //指定任务的reducer阶段，采用哪个类

        job.setMapOutputKeyClass(Text.class);            //指定map阶段的输出结果的键类型
        job.setMapOutputValueClass(IntWritable.class);    //指定map阶段的输出结果的值类型

        job.setOutputKeyClass(Text.class);                //指定reducer阶段的输出结果的键类型
        job.setOutputValueClass(IntWritable.class);    //指定reducer阶段的输出结果的值类型


        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));    //指定map阶段的输入数据来源于哪个文件夹
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));    //指定最终输出结果的目的地

        boolean b = job.waitForCompletion(true);        //把job任务，提交给yarn来运行
        System.exit(b ? 0 : 1);                                //如果运行mr程序正常，就正常退出虚拟机，否则就异常退出虚拟机
    }
}
