package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(
                10013,
                4,
                Constant.TOPIC_DWD_TRADE_CART_ADD
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       StreamTableEnvironment tEnv) {
        // 1. 读取 topic_db 数据
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_CART_ADD);
        // 2. 过滤出加购数据
        Table cartAdd = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id," +
                        " `data`['user_id'] user_id," +
                        " `data`['sku_id'] sku_id," +
                        " if(`type`='insert'," +
                        "   cast(`data`['sku_num'] as int), " +
                        "   cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)" +
                        ") sku_num ," +
                        " ts " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='cart_info' " +
                        "and (" +
                        " `type`='insert' " +
                        "  or(" +
                        "     `type`='update' " +
                        "      and `old`['sku_num'] is not null " +
                        "      and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int) " +
                        "   )" +
                        ")");

        // 3. 写出到 kafka
        tEnv.executeSql("create table dwd_trade_cart_add(" +
                "   id string, " +
                "   user_id string," +
                "   sku_id string," +
                "   sku_num int, " +
                "   ts  bigint " +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_CART_ADD));

        cartAdd.executeInsert(Constant.TOPIC_DWD_TRADE_CART_ADD);
    }
}
