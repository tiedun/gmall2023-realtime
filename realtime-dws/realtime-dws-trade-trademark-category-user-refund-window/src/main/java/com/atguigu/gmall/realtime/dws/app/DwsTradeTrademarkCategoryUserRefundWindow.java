package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeTrademarkCategoryUserRefundBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.AsyncDimFunction;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeTrademarkCategoryUserRefundWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeTrademarkCategoryUserRefundWindow().start(
                10031,
                4,
                "dws_trade_trademark_category_user_refund_window",
                Constant.TOPIC_DWD_TRADE_ORDER_REFUND
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> beanStream = stream
                .map(new MapFunction<String, TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public TradeTrademarkCategoryUserRefundBean map(String value) {
                        JSONObject obj = JSON.parseObject(value);
                        return TradeTrademarkCategoryUserRefundBean.builder()
                                .orderIdSet(new HashSet<>(Collections.singleton(obj.getString("order_id"))))
                                .skuId(obj.getString("sku_id"))
                                .userId(obj.getString("user_id"))
                                .ts(obj.getLong("ts") * 1000)
                                .build();
                    }
                });
        // 补充 keyBy 字段维度
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> reducedStream = AsyncDataStream
                .unorderedWait(
                        beanStream,
                        new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                                return bean.getSkuId();
                            }

                            @Override
                            public String getTableName() {
                                return "dim_sku_info";
                            }

                            @Override
                            public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                                bean.setTrademarkId(dim.getString("tm_id"));
                                bean.setCategory3Id(dim.getString("category3_id"));

                            }
                        },
                        120,
                        TimeUnit.SECONDS
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeTrademarkCategoryUserRefundBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120L))

                )
                .keyBy(bean -> bean.getUserId() + "_" + bean.getCategory3Id() + "_" + bean.getTrademarkId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradeTrademarkCategoryUserRefundBean>() {
                            @Override
                            public TradeTrademarkCategoryUserRefundBean reduce(TradeTrademarkCategoryUserRefundBean value1,
                                                                               TradeTrademarkCategoryUserRefundBean value2) throws Exception {
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeTrademarkCategoryUserRefundBean, TradeTrademarkCategoryUserRefundBean, String, TimeWindow>() {
                            @Override
                            public void process(String s,
                                                Context ctx,
                                                Iterable<TradeTrademarkCategoryUserRefundBean> elements,
                                                Collector<TradeTrademarkCategoryUserRefundBean> out) throws Exception {
                                TradeTrademarkCategoryUserRefundBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));

                                bean.setCurDate(DateFormatUtil.tsToDate(ctx.window().getStart()));  // doris 的分区字段: 年月日带连字符也可以

                                bean.setRefundCount((long) bean.getOrderIdSet().size());

                                out.collect(bean);
                            }
                        }
                );

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> tmStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean,
                                        JSONObject dim) {
                        bean.setTrademarkName(dim.getString("tm_name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c3Stream = AsyncDataStream.unorderedWait(
                tmStream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory3Name(dim.getString("name"));
                        bean.setCategory2Id(dim.getString("category2_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> c2Stream = AsyncDataStream.unorderedWait(
                c3Stream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory2Name(dim.getString("name"));
                        bean.setCategory1Id(dim.getString("category1_id"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );


        SingleOutputStreamOperator<TradeTrademarkCategoryUserRefundBean> resultStream = AsyncDataStream.unorderedWait(
                c2Stream,
                new AsyncDimFunction<TradeTrademarkCategoryUserRefundBean>() {
                    @Override
                    public String getRowKey(TradeTrademarkCategoryUserRefundBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDims(TradeTrademarkCategoryUserRefundBean bean, JSONObject dim) {
                        bean.setCategory1Name(dim.getString("name"));
                    }
                },
                120,
                TimeUnit.SECONDS
        );

        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_trade_trademark_category_user_refund_window", "dws_trade_trademark_category_user_refund_window"));

    }
}
