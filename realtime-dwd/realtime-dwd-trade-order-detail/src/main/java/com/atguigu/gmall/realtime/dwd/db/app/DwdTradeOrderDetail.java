package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(
                10014,
                4,
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       StreamTableEnvironment tEnv) {
        // 因为有 join: 默认所有表的数据都会一致存储到内存中. 所以要加 ttl
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5));

        // 1. 读取 topic_db
        readOdsDb(tEnv, Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);

        // 2. 过滤出 order_detail 数据: insert
        Table orderDetail = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['order_id'] order_id," +
                        "data['sku_id'] sku_id," +
                        "data['sku_name'] sku_name," +
                        "data['create_time'] create_time," +
                        "data['source_id'] source_id," +
                        "data['source_type'] source_type," +
                        "data['sku_num'] sku_num," +
                        "cast(cast(data['sku_num'] as decimal(16,2)) * " +
                        "   cast(data['order_price'] as decimal(16,2)) as String) split_original_amount," + // 分摊原始总金额
                        "data['split_total_amount'] split_total_amount," +  // 分摊总金额
                        "data['split_activity_amount'] split_activity_amount," + // 分摊活动金额
                        "data['split_coupon_amount'] split_coupon_amount," + // 分摊的优惠券金额
                        "ts " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_detail' " +
                        "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail", orderDetail);

        // 3. 过滤出 oder_info 数据: insert
        Table orderInfo = tEnv.sqlQuery(
                "select " +
                        "data['id'] id," +
                        "data['user_id'] user_id," +
                        "data['province_id'] province_id " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_info' " +
                        "and `type`='insert' ");
        tEnv.createTemporaryView("order_info", orderInfo);

        // 4. 过滤order_detail_activity 表: insert
        Table orderDetailActivity = tEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['activity_id'] activity_id, " +
                        "data['activity_rule_id'] activity_rule_id " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_detail_activity' " +
                        "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // 5. 过滤order_detail_coupon 表: insert
        Table orderDetailCoupon = tEnv.sqlQuery(
                "select " +
                        "data['order_detail_id'] order_detail_id, " +
                        "data['coupon_id'] coupon_id " +
                        "from topic_db " +
                        "where `database`='gmall' " +
                        "and `table`='order_detail_coupon' " +
                        "and `type`='insert' ");
        tEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);
        //orderDetailCoupon.execute().print();

        // 6. 四张表 join:
        Table result = tEnv.sqlQuery(
                "select " +
                        "od.id," +
                        "od.order_id," +
                        "oi.user_id," +
                        "od.sku_id," +
                        "od.sku_name," +
                        "oi.province_id," +
                        "act.activity_id," +
                        "act.activity_rule_id," +
                        "cou.coupon_id," +
                        "date_format(od.create_time, 'yyyy-MM-dd') date_id," +  // 年月日
                        "od.create_time," +
                        "od.sku_num," +
                        "od.split_original_amount," +
                        "od.split_activity_amount," +
                        "od.split_coupon_amount," +
                        "od.split_total_amount," +
                        "od.ts " +
                        "from order_detail od " +
                        "join order_info oi on od.order_id=oi.id " +
                        "left join order_detail_activity act " +
                        "on od.id=act.order_detail_id " +
                        "left join order_detail_coupon cou " +
                        "on od.id=cou.order_detail_id ");

        // 7. 写出到 kafka 中
        tEnv.executeSql(
                "create table dwd_trade_order_detail(" +
                        "id string," +
                        "order_id string," +
                        "user_id string," +
                        "sku_id string," +
                        "sku_name string," +
                        "province_id string," +
                        "activity_id string," +
                        "activity_rule_id string," +
                        "coupon_id string," +
                        "date_id string," +
                        "create_time string," +
                        "sku_num string," +
                        "split_original_amount string," +
                        "split_activity_amount string," +
                        "split_coupon_amount string," +
                        "split_total_amount string," +
                        "ts bigint," +
                        "primary key(id) not enforced " +
                        ")" + SQLUtil.getUpsertKafkaDDL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));

        result.executeInsert(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }
}
