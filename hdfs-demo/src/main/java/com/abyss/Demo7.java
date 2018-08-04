package com.abyss;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Demo7 {
	public static void main(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), new Configuration(), "root");
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus : listStatus) {
			Path path = fileStatus.getPath();
			boolean delete = fs.delete(path, true);
			System.out.println("delete = " + delete);
		}
		fs.close();
	}
}
