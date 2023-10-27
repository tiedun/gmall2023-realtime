package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeOrderWindow().start(
                10028,
                4,
                "dws_trade_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        stream
                .map(JSONObject::parseObject)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((obj, ts) -> obj.getLong("ts") * 1000)
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .keyBy(obj -> obj.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                    private ValueState<String> lastOrderDateState;

                    @Override
                    public void open(Configuration parameters) {
                        lastOrderDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastOrderDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject value,
                                               Context ctx,
                                               Collector<TradeOrderBean> out) throws Exception {
                        long ts = value.getLong("ts") * 1000;

                        String today = DateFormatUtil.tsToDate(ts);
                        String lastOrderDate = lastOrderDateState.value();

                        long orderUu = 0L;
                        long orderNew = 0L;
                        if (!today.equals(lastOrderDate)) {
                            orderUu = 1L;
                            lastOrderDateState.update(today);

                            if (lastOrderDate == null) {
                                orderNew = 1L;
                            }

                        }
                        if (orderUu == 1) {
                            out.collect(new TradeOrderBean("", "", "", orderUu, orderNew, ts));
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TradeOrderBean>() {
                            @Override
                            public TradeOrderBean reduce(TradeOrderBean value1,
                                                         TradeOrderBean value2) {
                                value1.setOrderUniqueUserCount(value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                                value1.setOrderNewUserCount(value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                                return value1;
                            }
                        },
                        new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<TradeOrderBean> elements,
                                                Collector<TradeOrderBean> out) throws Exception {
                                TradeOrderBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));

                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }
                )
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_trade_order_window", "dws_trade_order_window"));


    }
}
