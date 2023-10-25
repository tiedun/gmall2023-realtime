package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

@Slf4j
public class HBaseUtil {

    public static Connection getHBaseConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        return ConnectionFactory.createConnection(conf);
    }

    public static void closeHBaseConn(Connection hbaseConn) throws IOException {
        if (hbaseConn != null && !hbaseConn.isClosed()) {
            hbaseConn.close();
        }
    }

    public static void createHBaseTable(Connection hbaseConn,
                                        String nameSpace,
                                        String table,
                                        String family) throws IOException {
        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);
        // 判断要建的表是否存在
        if (admin.tableExists(tableName)) {
            return;
        }
        // 列族描述器
        ColumnFamilyDescriptor cfDesc = ColumnFamilyDescriptorBuilder.of(family);
        // 表的描述器
        TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(cfDesc) // 给表设置列族
                .build();
        admin.createTable(desc);
        admin.close();
        log.info(nameSpace + " " + table + " 建表成功");
    }

    public static void dropHBaseTable(Connection hbaseConn,
                                      String nameSpace,
                                      String table) throws IOException {

        Admin admin = hbaseConn.getAdmin();
        TableName tableName = TableName.valueOf(nameSpace, table);
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }
        admin.close();
        log.info(nameSpace + " " + table + " 删除成功");

    }

    /**
     * 用于向 HBase 目标表写入数据
     * @param conn HBase 连接对象
     * @param nameSpace HBase 命名空间
     * @param table HBase 表名
     * @param rowKey 数据的 row_key
     * @param family 数据所属的列族
     * @param data 待写出的数据
     * @throws IOException 可能抛出的异常
     */
    public static void putRow(Connection conn,
                              String nameSpace,
                              String table,
                              String rowKey,
                              String family,
                              JSONObject data) throws IOException {
        // 1. 获取 table 对象
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table t = conn.getTable(tableName);

        // 2. 创建 put 对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 3. 把每列放入 put 对象中
        for (String key : data.keySet()) {
            String value = data.getString(key);
            if (value != null) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(key), Bytes.toBytes(value));
            }
        }
        // 4. 向 table 对象中 put 数据
        t.put(put);

        t.close();
    }

    /**
     * 根据 row_key 删除 HBase 目标表指定数据
     * @param conn HBase 连接对象
     * @param nameSpace HBase 命名空间
     * @param table HBase 表名
     * @param rowKey HBase row_key
     * @throws IOException 可能抛出的异常
     */
    public static void delRow(Connection conn,
                              String nameSpace,
                              String table,
                              String rowKey) throws IOException {
        TableName tableName = TableName.valueOf(nameSpace, table);
        Table t = conn.getTable(tableName);
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        // 删除整行
        t.delete(delete);
        t.close();
    }

}
