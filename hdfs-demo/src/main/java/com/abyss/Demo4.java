package com.abyss;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Demo4 {
	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), new Configuration(), "root");
		FSDataOutputStream os = fs.create(new Path("/test4.log"), (short)3);
		os.write("who are you?".getBytes());
		os.close();
		fs.close();
	}
}
