package com.abyss.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HbaseUtils {

    private Connection connection;

    private HbaseUtils() {

    }

    public HbaseUtils(Connection connection) {
        this.connection = connection;
    }

    public HbaseUtils(Configuration configuration) throws IOException {
        this.connection = ConnectionFactory.createConnection(configuration);
    }

    public void deleteTable(String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        admin.close();
    }

    public void createTable(String tableName, String... familys) throws Exception {
        if (familys == null || familys.length == 0) {
            throw new Exception("列族名不能为空!");
        }
        // 从连接中获得一个Admin对象
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder tableDescriptorBuilder =
                TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for (String family : familys) {
            ColumnFamilyDescriptorBuilder userInfo = ColumnFamilyDescriptorBuilder.
                    newBuilder(Bytes.toBytes(family));
            userInfo.setMaxVersions(3); //设置版本信息
            tableDescriptorBuilder.setColumnFamily(userInfo.build());
        }
        admin.createTable(tableDescriptorBuilder.build());
        admin.close();
    }

    public void put(String tableName, String rowKey, String family, String column, String value) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        // 插入数据
        table.put(put);
        // 关闭连接
        table.close();
    }

    public void put(String tableName, String rowKey, List<ColumnData> list) throws Exception {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        for (ColumnData columnData : list) {
            put.addColumn(Bytes.toBytes(columnData.getFamily()), Bytes.toBytes(columnData.getColumn()), Bytes.toBytes(columnData.getValue()));
        }
        // 插入数据
        table.put(put);
        // 关闭连接
        table.close();
    }

    public List<ColumnData> findOne(String tableName, String rowKey) throws IOException {
        Table table = null;
        List<ColumnData> list = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            List<Cell> cells = result.listCells();
            list = new ArrayList<>();
            for (Cell cell : cells) {
                list.add(new ColumnData(cell));
            }
        } finally {
            if (null != table) {
                table.close();
            }
        }
        return list;
    }

    public List<Map<String, List<ColumnData>>> findAll(String tableName, int limit) throws IOException {
        return scan(tableName, null, limit);
    }

    public List<Map<String, List<ColumnData>>> findAll(String tableName) throws IOException {
        return scan(tableName, null, 0);
    }

    public List<Map<String, List<ColumnData>>> findAll(String tableName, String rowKeyPreFix) throws IOException {
        return findAll(tableName, rowKeyPreFix, 0);
    }

    public List<Map<String, List<ColumnData>>> findAll(String tableName, String rowKeyPreFix, int limit) throws IOException {
        PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(rowKeyPreFix));
        return scan(tableName, prefixFilter, limit);
    }

    private List<Map<String, List<ColumnData>>> scan(String tableName, Filter filter, int limit) throws IOException {
        Table table = null;
        List<Map<String, List<ColumnData>>> list = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            if (limit > 0) {
                scan.setLimit(limit);
            }
            if (null != filter) {
                scan.setFilter(filter);
            }
            ResultScanner scanner = table.getScanner(scan);
            Result result = null;
            list = new ArrayList<>();
            // 迭代数据
            while ((result = scanner.next()) != null) {
                Map<String, List<ColumnData>> map = new HashMap<>();
                List<Cell> cells = result.listCells();
                List<ColumnData> columnDataList = new ArrayList<>();
                if (cells != null && !cells.isEmpty()) {
                    for (Cell cell : cells) {
                        columnDataList.add(new ColumnData(cell));
                    }
                    map.put(Bytes.toString(CellUtil.cloneRow(cells.get(0))), columnDataList);
                }
                list.add(map);
            }
        } finally {
            if (null != table) {
                table.close();
            }
        }
        return list;
    }

    public static class ColumnData {
        private String family, column, value;

        public ColumnData(String family, String column, String value) {
            this.family = family;
            this.column = column;
            this.value = value;
        }

        public ColumnData(Cell cell) {
            this.family = Bytes.toString(CellUtil.cloneFamily(cell));
            this.column = Bytes.toString(CellUtil.cloneQualifier(cell));
            this.value = Bytes.toString(CellUtil.cloneValue(cell));
        }

        public ColumnData() {
        }

        public String getFamily() {
            return family;
        }

        public void setFamily(String family) {
            this.family = family;
        }

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "ColumnData{" +
                    "family='" + family + '\'' +
                    ", column='" + column + '\'' +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}