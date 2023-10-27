package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderRefund extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderRefund().start(
                10017,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       StreamTableEnvironment tEnv) {
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));
        // 1.1 读取 topic_db
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_REFUND);
        // 1.2 读取 字典表
        readBaseDic(tEnv);

        // 2. 过滤退单表数据 order_refund_info   insert
        Table orderRefundInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['refund_type'] refund_type," +
                        "data['refund_num'] refund_num," +
                        "data['refund_amount'] refund_amount," +
                        "data['refund_reason_type'] refund_reason_type," +
                        "data['refund_reason_txt'] refund_reason_txt," +
                        "data['create_time'] create_time," +
                        "pt," +
                        "ts " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_refund_info' " +
                        "and `type`='insert' ");
        tEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // 3. 过滤订单表中的退单数据: order_info  update
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['province_id'] province_id," +
                        "`old` " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_info' " +
                        "and `type`='update'" +
                        "and `old`['order_status'] is not null " +
                        "and `data`['order_status']='1005' ");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 4. join: 普通的和 lookup join
        Table result = tEnv.sqlQuery(
                "select " +
                        "ri.id," +
                        "ri.user_id," +
                        "ri.order_id," +
                        "ri.sku_id," +
                        "oi.province_id," +
                        "date_format(ri.create_time,'yyyy-MM-dd') date_id," +
                        "ri.create_time," +
                        "ri.refund_type," +
                        "dic1.info.dic_name," +
                        "ri.refund_reason_type," +
                        "dic2.info.dic_name," +
                        "ri.refund_reason_txt," +
                        "ri.refund_num," +
                        "ri.refund_amount," +
                        "ri.ts " +
                        "from order_refund_info ri " +
                        "join order_info oi " +
                        "on ri.order_id=oi.id " +
                        "join base_dic for system_time as of ri.pt as dic1 " +
                        "on ri.refund_type=dic1.dic_code " +
                        "join base_dic for system_time as of ri.pt as dic2 " +
                        "on ri.refund_reason_type=dic2.dic_code ");

        // 5. 写出到 kafka
        tEnv.executeSql(
                "create table dwd_trade_order_refund(" +
                        "id string," +
                        "user_id string," +
                        "order_id string," +
                        "sku_id string," +
                        "province_id string," +
                        "date_id string," +
                        "create_time string," +
                        "refund_type_code string," +
                        "refund_type_name string," +
                        "refund_reason_type_code string," +
                        "refund_reason_type_name string," +
                        "refund_reason_txt string," +
                        "refund_num string," +
                        "refund_amount string," +
                        "ts bigint " +
                        ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_TRADE_ORDER_REFUND));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_REFUND);


    }
}
