package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.UserLoginBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.DorisMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(
                10024,
                4,
                "dws_user_user_login_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 先过滤出所有登录记录
        SingleOutputStreamOperator<JSONObject> loginLogStream = filterLoginLog(stream);

        // 2. 再解析封装到 pojo 中: 如果是当然首次登录, 则置为 1 否则置为 0
        // 回流用户: 判断今天和最后一次登录日期的差值是否大于 7
        SingleOutputStreamOperator<UserLoginBean> beanStream = parseToPojo(loginLogStream);

        // 3.开窗聚合
        SingleOutputStreamOperator<UserLoginBean> resultStream = windowAndAgg(beanStream);

        // 4. 写出
        writeToDoris(resultStream);
    }

    private void writeToDoris(SingleOutputStreamOperator<UserLoginBean> resultStream) {
        resultStream
                .map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DORIS_DATABASE + ".dws_user_user_login_window", "dws_user_user_login_window"));
    }

    private SingleOutputStreamOperator<UserLoginBean> windowAndAgg(SingleOutputStreamOperator<UserLoginBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<UserLoginBean>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120L))
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<UserLoginBean>() {
                            @Override
                            public UserLoginBean reduce(UserLoginBean value1,
                                                        UserLoginBean value2) throws Exception {
                                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                                return value1;
                            }
                        },
                        new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                            @Override
                            public void apply(TimeWindow window,
                                              Iterable<UserLoginBean> values,
                                              Collector<UserLoginBean> out) throws Exception {
                                UserLoginBean bean = values.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));

                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(window.getStart()));

                                out.collect(bean);
                            }
                        }
                );
    }

    private SingleOutputStreamOperator<UserLoginBean> parseToPojo(SingleOutputStreamOperator<JSONObject> stream) {
        return stream
                .keyBy(obj -> obj.getJSONObject("common").getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {

                    private ValueState<String> lastLoginDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastLoginDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<UserLoginBean> out) throws Exception {
                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);

                        String lastLoginDate = lastLoginDateState.value();

                        Long uuCt = 0L;
                        Long backCt = 0L;
                        if (!today.equals(lastLoginDate)) {
                            // 今天的第一次登录
                            uuCt = 1L;
                            lastLoginDateState.update(today);

                            // 计算回流: 曾经登录过
                            if (lastLoginDate != null) { //
                                long lastLoginTs = DateFormatUtil.dateToTs(lastLoginDate);

                                // 7日回流
                                if ((ts - lastLoginTs) / 1000 / 60 / 60 / 24 > 7) {
                                    backCt = 1L;
                                }
                            }
                        }

                        if (uuCt == 1) {
                            out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                        }

                    }
                });
    }

    private SingleOutputStreamOperator<JSONObject> filterLoginLog(DataStreamSource<String> stream) {
        return stream
                .map(JSON::parseObject)
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                    /*
                    登录日志:
                        自动登录
                            uid != null && last_page_id == null

                        手动登录
                            A -> 登录页面 -> B(登录成功)

                            udi != null && last_page_id = login

                        合并优化: uid != null && (last_page_id == null || last_page_id = login)
                     */
                        String uid = value.getJSONObject("common").getString("uid");
                        String lastPageId = value.getJSONObject("page").getString("last_page_id");

                        return uid != null && (lastPageId == null || "login".equals(lastPageId));
                    }

                });
    }
}
