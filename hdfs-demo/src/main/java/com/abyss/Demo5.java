package com.abyss;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Demo5 {
	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), new Configuration(), "root");
		boolean delete = fs.delete(new Path("/vvv"), true);
		System.out.println("delete = " + delete);
		fs.close();
	}
}
