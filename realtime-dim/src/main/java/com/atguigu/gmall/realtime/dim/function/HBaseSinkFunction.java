package com.atguigu.gmall.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDim;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

@Slf4j
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {

    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseUtil.closeHBaseConn(conn);
    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> dataWithConfig,
                       Context context) throws Exception {
        // insert update delete bootstrap-insert
        JSONObject data = dataWithConfig.f0;
        String opType = data.getString("op_type");

        if ("delete".equals(opType)) {
            // 删除维度信息
            delDim(dataWithConfig);
        }else{
            // insert update 和 bootstrap-insert 的时候, 写入维度数据
            putDim(dataWithConfig);
        }
    }

    private void putDim(Tuple2<JSONObject, TableProcessDim> dataWithConfig) throws IOException {
        JSONObject data = dataWithConfig.f0;
        TableProcessDim tableProcessDim = dataWithConfig.f1;

        String rowKey = data.getString(tableProcessDim.getSinkRowKey());
        log.info("向 HBase 写入数据 dataWithConfig: " + dataWithConfig);
        // data中有多少 kv 就写多少列 - 1
        data.remove("op_type");
        HBaseUtil.putRow(conn,
                Constant.HBASE_NAMESPACE,
                tableProcessDim.getSinkTable(),
                rowKey,
                tableProcessDim.getSinkFamily(),
                data);

    }

    private void delDim(Tuple2<JSONObject, TableProcessDim> dataWithConfig) throws IOException {
        JSONObject data = dataWithConfig.f0;
        TableProcessDim tableProcessDim = dataWithConfig.f1;

        String rowKey = data.getString(tableProcessDim.getSinkRowKey());

        HBaseUtil.delRow(conn,
                Constant.HBASE_NAMESPACE,
                tableProcessDim.getSinkTable(),
                rowKey);
    }
}
