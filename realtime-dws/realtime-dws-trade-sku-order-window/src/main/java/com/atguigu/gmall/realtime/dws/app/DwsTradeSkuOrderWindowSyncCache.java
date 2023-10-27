package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.dws.function.MapDimFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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

import java.math.BigDecimal;
import java.time.Duration;

public class DwsTradeSkuOrderWindowSyncCache extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowSyncCache().start(
                60009,
                1,
                "dws_trade_sku_order_window",
                Constant.TOPIC_DWD_TRADE_ORDER_DETAIL
        );

    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        // 1. 解析成 pojo 类型
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStream = parseToPojo(stream);

        // 2. 按照 order_detail_id 去重
        beanStream = distinctByOrderId(beanStream);

        // 3. 按照 sku_id 分组, 开窗, 聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> beanStreamWithoutDims = windowAndAgg(beanStream);

        // 4. join 维度
        joinDim(beanStreamWithoutDims);

    }

    private void joinDim(SingleOutputStreamOperator<TradeSkuOrderBean> stream) {
        /*
            每来一条数据, 都需要去维度层(hbase)中查找对应的维度, 补充到 bean 中


            6 张维度表:
                dim_sku_info  sku_id
                    sku_name spu_id tm_id c3_id
                dim_spu_info spu_id
                    spu_name
                dim_base_trademark tm_id
                    tm_name
                dim_base_category3  c3_id
                    c3_name c2_id
                dim_base_category2 c2_id
                    c2_name c1_id
                dim_base_category3 c1_id
                    c1_name

         */
        // 补充 sku 信息
        SingleOutputStreamOperator<TradeSkuOrderBean> skuSteam = stream
                .map(new MapDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getSkuId();
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean,
                                        JSONObject dim) {
                        bean.setSkuName(dim.getString("sku_name"));
                        bean.setSpuId(dim.getString("spu_id"));
                        bean.setTrademarkId(dim.getString("tm_id"));
                        bean.setCategory3Id(dim.getString("category3_id"));
                    }

                });
        SingleOutputStreamOperator<TradeSkuOrderBean> spuStream = skuSteam.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getSpuId();
            }

            @Override
            public String getTableName() {
                return "dim_spu_info";
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setSpuName(dim.getString("spu_name"));

            }
        });

        SingleOutputStreamOperator<TradeSkuOrderBean> tmStream = spuStream
                .map(new MapDimFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean bean) {
                        return bean.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDims(TradeSkuOrderBean bean,
                                        JSONObject dim) {
                        bean.setTrademarkName(dim.getString("tm_name"));
                    }
                });

        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = tmStream.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory3Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category3";
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setCategory3Name(dim.getString("name"));
                bean.setCategory2Id(dim.getString("category2_id"));
            }
        });


        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = c3Stream.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory2Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category2";
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setCategory2Name(dim.getString("name"));
                bean.setCategory1Id(dim.getString("category1_id"));
            }
        });

        SingleOutputStreamOperator<TradeSkuOrderBean> c1Stream = c2Stream.map(new MapDimFunction<TradeSkuOrderBean>() {
            @Override
            public String getRowKey(TradeSkuOrderBean bean) {
                return bean.getCategory1Id();
            }

            @Override
            public String getTableName() {
                return "dim_base_category1";
            }

            @Override
            public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
                bean.setCategory1Name(dim.getString("name"));
            }
        });

        c1Stream.print();

    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> windowAndAgg(
            SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
        return beanStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((bean, ts) -> bean.getTs())
                                .withIdleness(Duration.ofSeconds(120))
                )
                .keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TradeSkuOrderBean>() {
                            @Override
                            public TradeSkuOrderBean reduce(TradeSkuOrderBean value1,
                                                            TradeSkuOrderBean value2) throws Exception {
                                value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                                value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                                value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                                value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));

                                return value1;
                            }

                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String skuId,
                                                Context ctx,
                                                Iterable<TradeSkuOrderBean> elements,
                                                Collector<TradeSkuOrderBean> out) throws Exception {
                                TradeSkuOrderBean bean = elements.iterator().next();
                                bean.setStt(DateFormatUtil.tsToDateTime(ctx.window().getStart()));
                                bean.setEdt(DateFormatUtil.tsToDateTime(ctx.window().getEnd()));
                                bean.setCurDate(DateFormatUtil.tsToDateForPartition(ctx.window().getEnd()));

                                out.collect(bean);
                            }
                        }
                );

    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> distinctByOrderId(
            SingleOutputStreamOperator<TradeSkuOrderBean> beanStream) {
        /*
        去重逻辑: 按照 order_detail_id 分组
            思路: 1
                使用 session 窗口, 当窗口关闭的时候, 同一个详情的多条数据,一定都来齐了.找到最完整的那个.
                    需要 dwd 层的数据添加一个字段,表示这条数据的生成时间, 时间大的那个就是最完整的
                        详情id  sku_id  金额   活动      优惠券  系统时间
                          1      100    100  null      null     1
                          null
                          1     100   100   有值      null       2
                          1     100   100   有值      有值        3

                   优点: 简单
                   缺点: 实效性低
                        窗口内的最后一条数据到了之后,经过一个 gap(5s)才能计算出结果

             思路: 2
                定时器
                    当第一条数据到的时候, 注册一个 5s 后触发的定时器, 把这条存入到状态中
                    每来一条数据, 就和状态中的比较时间, 时间大的保存到状态中.
                    当定时器触发的时候, 则状态中存储的一定是时间最大的那个

                    优点: 简单
                    确定: 实效性低
                        窗口内的第一条数据到了之后,5s才能计算出结果

             思路: 3  抵消
                            详情id  sku_id  左表金额  右表1活动    右表2优惠券
                   第一条     1      100    100       null      null       直接输出
                             1      100    -100      null      null       直接输出
                   第二条     1      100    100       200      null        直接输出
                             1      100     -100      -200      null      直接输出
                   第三条     1      100    100       200      300         直接输出
                优点: 实效性高
                缺点: 写放大

                    优化:
                            详情id  sku_id    左表金额  右表1活动    右表2优惠券
                   第一条     1      100      100       null      null       直接输出
                   第二条     1      100     100+(-100)  200      null        直接输出
                   第三条     1      100    100+(-100) 200+(-200)      300    直接输出
                 优点: 实效性高

               思路 4:
                    如果需要聚合的值都在左表, 不需要等最后最完整的数据.

                    只需要输出第一条就行了.

                    优点: 实效性最高
                    缺点: 特殊情况. 聚和的值都在左表


         */
        return beanStream
                .keyBy(TradeSkuOrderBean::getOrderDetailId)
                .process(new KeyedProcessFunction<String, TradeSkuOrderBean, TradeSkuOrderBean>() {

                    private ValueState<TradeSkuOrderBean> lastBeanState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<TradeSkuOrderBean> des = new ValueStateDescriptor<>("lastBean", TradeSkuOrderBean.class);
                        StateTtlConfig conf = new StateTtlConfig.Builder(Time.seconds(60))
                                .useProcessingTime()
                                .updateTtlOnCreateAndWrite()
                                .neverReturnExpired()
                                .build();
                        des.enableTimeToLive(conf);
                        lastBeanState = getRuntimeContext().getState(des);
                    }

                    @Override
                    public void processElement(TradeSkuOrderBean currentBean,
                                               Context ctx,
                                               Collector<TradeSkuOrderBean> out) throws Exception {
                        TradeSkuOrderBean lastBean = lastBeanState.value();
                        if (lastBean == null) { // 第1条数据来了, 则直接输出
                            out.collect(currentBean);
                        } else {
                            // 把上条数据的值取反, 然后和当前的 bean 中的值相加, 输出计算后的值
                            lastBean.setOrderAmount(currentBean.getOrderAmount().subtract(lastBean.getOrderAmount()));
                            lastBean.setOriginalAmount(currentBean.getOriginalAmount().subtract(lastBean.getOriginalAmount()));
                            lastBean.setActivityReduceAmount(currentBean.getActivityReduceAmount().subtract(lastBean.getActivityReduceAmount()));
                            lastBean.setCouponReduceAmount(currentBean.getCouponReduceAmount().subtract(lastBean.getCouponReduceAmount()));
                            out.collect(lastBean);
                        }

                        // 把当前的 bean 更新到状态中
                        lastBeanState.update(currentBean);

                    }
                });

    }

    private SingleOutputStreamOperator<TradeSkuOrderBean> parseToPojo(DataStreamSource<String> stream) {
        return stream
                .map(new MapFunction<String, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(String value) throws Exception {
                        JSONObject obj = JSON.parseObject(value);

                        return TradeSkuOrderBean.builder()
                                .skuId(obj.getString("sku_id"))
                                .orderDetailId(obj.getString("id"))
                                .originalAmount(obj.getBigDecimal("split_original_amount"))
                                .orderAmount(obj.getBigDecimal("split_total_amount"))
                                .activityReduceAmount(obj.getBigDecimal("split_activity_amount") == null ? new BigDecimal("0.0") : obj.getBigDecimal("split_activity_amount"))
                                .couponReduceAmount(obj.getBigDecimal("split_coupon_amount") == null ? new BigDecimal("0.0") : obj.getBigDecimal("split_coupon_amount"))
                                .ts(obj.getLong("ts") * 1000)
                                .build();
                    }

                });
    }
}
