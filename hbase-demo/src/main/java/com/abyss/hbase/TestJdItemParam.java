package com.abyss.hbase;

import com.abyss.hbase.utils.HbaseUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TestJdItemParam {

    private HbaseUtils hbaseUtils;

    @Before
    public void init() throws IOException {
        Configuration configuration = new Configuration();
        // 配置ZooKeeper信息
        configuration.set("hbase.zookeeper.quorum", "node1:2181");
        this.hbaseUtils = new HbaseUtils(configuration);
    }

    @Test
    public void testCreateTable() throws Exception {
        String[] familys = new String[]{"F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "F10", "F11", "F12", "F13", "F14", "F15"};
        this.hbaseUtils.createTable("tb_jd_param", familys);
        System.out.println("创建表成功!");
    }

    @Test
    public void testPut() throws Exception {
        //String tableName, String rowKey, String family, String column, String value

        // PARAM_分类ID_商品ID
        String rowKey = "PARAM_108_7652142";
        this.hbaseUtils.put("tb_jd_param", rowKey, "F1", "主体:品牌", "小米（MI）");
        this.hbaseUtils.put("tb_jd_param", rowKey, "F1", "主体:型号", "小米8");
        this.hbaseUtils.put("tb_jd_param", rowKey, "F2", "基本信息:机身颜色", "黑色");
        this.hbaseUtils.put("tb_jd_param", rowKey, "F3", "操作系统:操作系统", "Android");

        String rowKey2 = "PARAM_103_4609653";
        this.hbaseUtils.put("tb_jd_param", rowKey2, "F1", "主体参数:产品品牌", "小米（MI）");
        this.hbaseUtils.put("tb_jd_param", rowKey2, "F1", "主体参数:上市日期", "2017.3");
        this.hbaseUtils.put("tb_jd_param", rowKey2, "F2", "显示参数:HDR显示", "支持");
        this.hbaseUtils.put("tb_jd_param", rowKey2, "F3", "外观设计:机身厚薄", "最薄处：12.3mm");


        String rowKey3 = "PARAM_103_5008018";
        this.hbaseUtils.put("tb_jd_param", rowKey3, "F1", "主体参数:产品品牌", "三星");
        this.hbaseUtils.put("tb_jd_param", rowKey3, "F1", "主体参数:上市日期", "2018.3");
        this.hbaseUtils.put("tb_jd_param", rowKey3, "F2", "显示参数:HDR显示", "不支持");
        this.hbaseUtils.put("tb_jd_param", rowKey3, "F3", "外观设计:机身厚薄", "最薄处：11.3mm");
    }

    @Test
    public void testFindOne() throws Exception {
        String tableName = "tb_jd_param";
        // PARAM_分类ID_商品ID
        String rowKey = "PARAM_108_7652142";
        List<HbaseUtils.ColumnData> list = this.hbaseUtils.findOne(tableName, rowKey);
        for (HbaseUtils.ColumnData columnData : list) {
            System.out.println(columnData);
        }
    }

    @Test
    public void testFindAllRowKeyPreFix() throws Exception {
        String tableName = "tb_jd_param";
        List<Map<String, List<HbaseUtils.ColumnData>>> all = this.hbaseUtils.findAll(tableName,"PARAM");
        for (Map<String, List<HbaseUtils.ColumnData>> map : all) {
            for (Map.Entry<String, List<HbaseUtils.ColumnData>> entry : map.entrySet()) {
                String rowKey = entry.getKey();
                List<HbaseUtils.ColumnData> list = entry.getValue();
                for (HbaseUtils.ColumnData columnData : list) {
                    System.out.println(rowKey + "-->" + columnData);
                }
            }
        }
    }


}
