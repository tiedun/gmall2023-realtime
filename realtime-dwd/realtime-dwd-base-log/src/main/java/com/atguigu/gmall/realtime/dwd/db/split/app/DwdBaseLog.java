package com.atguigu.gmall.realtime.dwd.db.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DwdBaseLog extends BaseApp {

    private final String START = "start";
    private final String ERR = "err";
    private final String DISPLAY = "display";
    private final String ACTION = "action";
    private final String PAGE = "page";

    public static void main(String[] args) {
        new DwdBaseLog().start(
                10011,
                4,
                "dwd_base_log",
                Constant.TOPIC_LOG
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env,
                       DataStreamSource<String> stream) {
        // 1. etl
        SingleOutputStreamOperator<JSONObject> etledStream = etl(stream);

        // 2. 纠正新老客户
        SingleOutputStreamOperator<JSONObject> validatedStream = validateNewOrOld(etledStream);
        // 3. 分流
        Map<String, DataStream<JSONObject>> streams = splitStream(validatedStream);

        // 4. 不同的流写出到不同的 topic
        writeToKafka(streams);

    }

    private void writeToKafka(Map<String, DataStream<JSONObject>> streams) {
        streams
                .get(START)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));

        streams
                .get(ERR)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));

        streams
                .get(DISPLAY)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));

        streams
                .get(PAGE)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));

        streams
                .get(ACTION)
                .map(JSONAware::toJSONString)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    private Map<String, DataStream<JSONObject>> splitStream(SingleOutputStreamOperator<JSONObject> stream) {
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display"){};
        OutputTag<JSONObject> actionTag = new OutputTag<JSONObject>("action"){};
        OutputTag<JSONObject> errTag = new OutputTag<JSONObject>("err"){};
        OutputTag<JSONObject> pageTag = new OutputTag<JSONObject>("page"){};
        /*
        主流: 启动日志
        侧输出流: 页面 错误 曝光 活动
         */
        SingleOutputStreamOperator<JSONObject> startStream = stream
                .process(new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {
                        JSONObject common = obj.getJSONObject("common");
                        Long ts = obj.getLong("ts");
                        // 1. 启动
                        JSONObject start = obj.getJSONObject("start");
                        if (start != null) {
                            out.collect(obj);
                        }

                        // 2. 曝光
                        JSONArray displays = obj.getJSONArray("displays");
                        if (displays != null) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject display = displays.getJSONObject(i);
                                display.putAll(common);
                                display.put("ts", ts);
                                ctx.output(displayTag, display);
                            }

                            // 删除displays
                            obj.remove("displays");
                        }
                        // 3. 活动
                        JSONArray actions = obj.getJSONArray("actions");
                        if (actions != null) {
                            for (int i = 0; i < actions.size(); i++) {
                                JSONObject action = actions.getJSONObject(i);
                                action.putAll(common);
                                ctx.output(actionTag, action);
                            }

                            // 删除displays
                            obj.remove("actions");
                        }

                        // 4. err
                        JSONObject err = obj.getJSONObject("err");
                        if (err != null) {
                            ctx.output(errTag, obj);
                            obj.remove("err");
                        }

                        // 5. 页面
                        JSONObject page = obj.getJSONObject("page");
                        if (page != null) {
                            ctx.output(pageTag, obj);
                        }

                    }
                });

        Map<String, DataStream<JSONObject>> streams = new HashMap<>();

        streams.put(START, startStream);
        streams.put(DISPLAY, startStream.getSideOutput(displayTag));
        streams.put(ERR, startStream.getSideOutput(errTag));
        streams.put(PAGE, startStream.getSideOutput(pageTag));
        streams.put(ACTION, startStream.getSideOutput(actionTag));

        return streams;

    }

    private SingleOutputStreamOperator<JSONObject> validateNewOrOld(SingleOutputStreamOperator<JSONObject> stream) {
        return  stream
                .keyBy(obj -> obj.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> firstVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDateState = getRuntimeContext().getState(new ValueStateDescriptor<String>("firstVisitDate", String.class));
                    }

                    @Override
                    public void processElement(JSONObject obj,
                                               Context ctx,
                                               Collector<JSONObject> out) throws Exception {

                        JSONObject common = obj.getJSONObject("common");
                        String isNew = common.getString("is_new");

                        Long ts = obj.getLong("ts");
                        String today = DateFormatUtil.tsToDate(ts);


                        // 从状态中获取首次访问日志
                        String firstVisitDate = firstVisitDateState.value();

                        if ("1".equals(isNew)) {
                            if (firstVisitDate == null) {
                                // 这个 mid 的首次访问
                                firstVisitDateState.update(today);
                            }else if(!today.equals(firstVisitDate)){
                                // 今天和首次访问不一致
                                common.put("is_new", "0");  // 把新用户修改为老用户
                            }
                        }else{
                            if (firstVisitDate == null) {
                                // 一个老用户, 他的首次访问日志还是 null
                                // 把他的首次访问日期设置为昨天
                                firstVisitDateState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000));
                            }
                        }
                        out.collect(obj);
                    }
                });

    }

    private SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        try {
                            JSON.parseObject(value );
                            return true;
                        } catch (Exception e) {
                            log.error("日志格式不是正确的 JSON 格式: " + value);
                            return false;
                        }
                    }
                })
                .map(JSON::parseObject);
    }
}
