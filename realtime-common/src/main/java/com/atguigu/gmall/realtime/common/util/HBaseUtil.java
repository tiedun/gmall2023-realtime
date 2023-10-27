package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

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

    /**
     * 根据参数从 hbase 指定的表中查询一行数据
     *
     * @param hbaseConn hbase 链接
     * @param nameSpace 命名空间
     * @param table     表面
     * @param rowKey    rowKey
     * @return 把一行查询到的所有列封装到一个 JSONObject 对象中
     */
    public static <T> T getRow(Connection hbaseConn,
                               String nameSpace,
                               String table,
                               String rowKey,
                               Class<T> tClass,
                               boolean... isUnderlineToCamel) {
        boolean defaultIsUToC = false;  // 默认不执行下划线转驼峰

        if (isUnderlineToCamel.length > 0) {
            defaultIsUToC = isUnderlineToCamel[0];
        }

        try (Table Table = hbaseConn.getTable(TableName.valueOf(nameSpace, table))) { // jdk1.7 : 可以自动释放资源
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = Table.get(get);
            // 4. 把查询到的一行数据,封装到一个对象中: JSONObject
            // 4.1 一行中所有的列全部解析出来
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            T t = tClass.newInstance();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (defaultIsUToC) { // 需要下划线转驼峰:  a_a => aA a_aaaa_aa => aAaaaAa
                    key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, key);
                }
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                BeanUtils.setProperty(t, key, value);
            }
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取到 Hbase 的异步连接
     *
     * @return 得到异步连接对象
     */
    public static AsyncConnection getHBaseAsyncConnection() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "hadoop102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try {
            return ConnectionFactory.createAsyncConnection(conf).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭 hbase 异步连接
     *
     * @param asyncConn 异步连接
     */
    public static void closeAsyncHbaseConnection(AsyncConnection asyncConn) {
        if (asyncConn != null) {
            try {
                asyncConn.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 异步的从 hbase 读取维度数据
     *
     * @param hBaseAsyncConn hbase 的异步连接
     * @param nameSpace      命名空间
     * @param tableName      表名
     * @param rowKey         rowKey
     * @return 读取到的维度数据, 封装到 json 对象中.
     */
    public static JSONObject readDimAsync(AsyncConnection hBaseAsyncConn,
                                          String nameSpace,
                                          String tableName,
                                          String rowKey) {
        AsyncTable<AdvancedScanResultConsumer> asyncTable = hBaseAsyncConn
                .getTable(TableName.valueOf(nameSpace, tableName));

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            // 获取 result
            Result result = asyncTable.get(get).get();
            List<Cell> cells = result.listCells();  // 一个 Cell 表示这行中的一列
            JSONObject dim = new JSONObject();
            for (Cell cell : cells) {
                // 取出每列的列名(json 对象的中的 key)和列值(json 对象中的 value)
                String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                dim.put(key, value);
            }

            return dim;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }
}
