package com.abyss;


/**
 * Created by Abyss on 2018/7/26.
 * description:
 */
public class ConfigurationTest {
  /* @Test  //需求：创建一个目录，测试以下环境
    public void demo1() throws Exception{
        Configuration con = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), con, "root");

        boolean mkdirs = fs.mkdirs(new Path("/ppp/bbb/ccc"));
        System.out.println("mkdirs = " + mkdirs);


        fs.close();
    }*/

/*    @Test  //需求：把数据写入 /www.log
    public void demo2() throws Exception{
        Configuration con = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://node1:9000"), con, "root");

        FSDataOutputStream os = fs.create(new Path("/www.log"));
        os.write("hello , world".getBytes());


        fs.close();
    }*/

}
