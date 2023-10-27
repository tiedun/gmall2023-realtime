package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.api.common.state.StateTtlConfig.StateVisibility.NeverReturnExpired;
import static org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite;

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(
                10022,
                4,
                "dws_traffic_vc_ch_ar_is_new_page_view_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 解析数据, 封装 pojo 中
        SingleOutputStreamOperator<TrafficPageViewBean> beanStream = parseToPojo(stream);
        // 2. 开窗聚合
        SingleOutputStreamOperator<TrafficPageViewBean> result = windowAndAgg(beanStream);

        // 3. 写出到 doris 中
        writeToDoris(result);
    }

    private void writeToDoris(SingleOutputStreamOperator<TrafficPageViewBean> result) {

        result
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_traffic_vc_ch_ar_is_new_page_view_window", "dws_traffic_vc_ch_ar_is_new_page_view_window"));

    }

    private SingleOutputStreamOperator<TrafficPageViewBean> windowAndAgg(
            SingleOutputStreamOperator<TrafficPageViewBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .keyBy(bean -> bean.getVc() + "_" + bean.getCh() + "_" + bean.getAr() + "_" + bean.getIsNew())
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean value1,
                                                              TrafficPageViewBean value2) throws Exception {
                                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<TrafficPageViewBean> elements, // 有且仅有一个值: 前面聚合的最终结果
                                                Collector<TrafficPageViewBean> out) throws Exception {
                                TrafficPageViewBean bean = elements.iterator().next();

                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCur_date(DateFormatUtil.tsToDateForPartition(ctx.window().getStart()));

                                out.collect(bean);
                            }
                        }

                );
    }

    private SingleOutputStreamOperator<TrafficPageViewBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))  // 顺便计算下 uv, 按照 mid 进行分组, 找到同一个 mid 的第一条记录, uv 置为 1, 其他的uv为 0
                .process(new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {

                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastVisitDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<TrafficPageViewBean> out) throws Exception {
                        JSONObject page = obj.getJSONObject("page");
                        JSONObject common = obj.getJSONObject("common");
                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        Long pv = 1L;
                        Long durSum = page.getLong("during_time");
                        // uv
                        String lastVisitDate = lastVisitDateState.value();
                        Long uv = 0L;
                        if (!today.equals(lastVisitDate)) {
                            // 是当天的第一次
                            uv = 1L;
                            // 更新状态
                            lastVisitDateState.update(today);
                        }

                        TrafficPageViewBean bean = new TrafficPageViewBean();
                        bean.setVc(common.getString("vc"));
                        bean.setCh(common.getString("ch"));
                        bean.setAr(common.getString("ar"));
                        bean.setIsNew(common.getString("is_new"));

                        bean.setPvCt(pv);
                        bean.setUvCt(uv);
                        bean.setDurSum(durSum);
                        bean.setTs(ts);

                        bean.setSid(common.getString("sid"));

                        out.collect(bean);

                    }
                })
                .keyBy(TrafficPageViewBean::getSid)  //按照 sid 分组, 计算 sv 使用
                .process(new KeyedProcessFunction<String, TrafficPageViewBean, TrafficPageViewBean>() {

                    private ValueState<Boolean> isFirstState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("isFirst", Boolean.class);
                        StateTtlConfig conf = new StateTtlConfig.Builder(Time.hours(1))
                                .setStateVisibility(NeverReturnExpired)
                                .setUpdateType(OnCreateAndWrite)
                                .useProcessingTime()
                                .build();
                        desc.enableTimeToLive(conf);
                        isFirstState = getRuntimeContext().getState(desc);

                    }

                    @Override
                    public void processElement(TrafficPageViewBean bean,
                                               Context ctx,
                                               Collector<TrafficPageViewBean> out) throws Exception {

                        if (isFirstState.value() == null) {
                            bean.setSvCt(1L);
                            isFirstState.update(true);
                        } else {
                            bean.setSvCt(0L);
                        }
                        out.collect(bean);
                    }
                });
    }
}
