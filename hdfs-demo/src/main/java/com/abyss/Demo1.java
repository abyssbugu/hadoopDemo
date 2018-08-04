package com.abyss;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Demo1 {
	
	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), new Configuration(), "root");
		boolean mkdirs = fs.mkdirs(new Path("/我爱你/你爱他"));
		System.out.println("mkdirs = " + mkdirs);
		fs.close();
	}

}
