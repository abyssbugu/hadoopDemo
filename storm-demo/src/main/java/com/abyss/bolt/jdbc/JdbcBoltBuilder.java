package com.abyss.bolt.jdbc;

import com.google.common.collect.Lists;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.topology.IRichBolt;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Abyss on 2018/8/14.
 * description:
 */
public class JdbcBoltBuilder {

    private JdbcBoltBuilder() {

    }

    public static IRichBolt build() {
        // 定义数据库连接信息
        Map<String, Object> hikariConfigMap = new HashMap<String, Object>();
        hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost:3306/storm");
        hikariConfigMap.put("dataSource.user", "root");
        hikariConfigMap.put("dataSource.password", "000000");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        // 定义表名，以及定义字段的映射，这里指定的是tupe中的字段名称，用于获取数据
        List<Column> columnSchema = Lists.newArrayList(
                new Column("word", Types.VARCHAR),
                new Column("count", Types.INTEGER));

        // 定义jdbc的映射器
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);

        // 定义插入数据的Bolt，并且指定了插入的sql语句
        JdbcInsertBolt wordCountBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withInsertQuery("INSERT INTO `tb_wordcount` VALUES (NULL, ?, ?, NOW())")
                .withQueryTimeoutSecs(30);

        return wordCountBolt;
    }
}
