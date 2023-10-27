package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdInteractionCommentInfo extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdInteractionCommentInfo().start(
                10012,
                4,
                Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       StreamTableEnvironment tEnv) {

        // 1. 通过 ddl 的方式建立动态表: 从 topic_db 读取数据 (source)
        readOdsDb(tEnv, Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
        // 2. 过滤出评论表数据
        Table commentInfo = tEnv.sqlQuery(
                "select " +
                        " `data`['id'] id, " +
                        " `data`['user_id'] user_id, " +
                        " `data`['sku_id'] sku_id, " +
                        " `data`['appraise'] appraise, " +
                        " `data`['comment_txt'] comment_txt, " +
                        " `data`['create_time'] comment_time," +
                        " ts, " +
                        " pt " +
                        " from topic_db " +
                        " where `database`='gmall' " +
                        " and `table`='comment_info' " +
                        " and `type`='insert' ");
        tEnv.createTemporaryView("comment_info", commentInfo);

        // 3. 通过 ddl 方式建表: base_dic hbase 中的维度表 (source)
        readBaseDic(tEnv);
        // 4. 事实表与维度表的 join: lookup join
        Table result = tEnv.sqlQuery("select " +
                "ci.id, " +
                "ci.user_id," +
                "ci.sku_id," +
                "ci.appraise," +
                "dic.info.dic_name appraise_name," +
                "ci.comment_txt," +
                "ci.ts " +
                "from comment_info ci " +
                "join base_dic for system_time as of ci.pt as dic " +
                "on ci.appraise=dic.dic_code");

        // 5. 通过 ddl 方式建表: 与 kafka 的 topic 管理 (sink)
        tEnv.executeSql("create table dwd_interaction_comment_info(" +
                "id string, " +
                "user_id string," +
                "sku_id string," +
                "appraise string," +
                "appraise_name string," +
                "comment_txt string," +
                "ts bigint " +
                ")" + SQLUtil.getKafkaDDLSink(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO));

        // 6. 把 join 的结果写入到 sink 表
        result.executeInsert(Constant.TOPIC_DWD_INTERACTION_COMMENT_INFO);
    }
}
