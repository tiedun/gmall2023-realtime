package com.atguigu.gmall.realtime.common.base;

import com.atguigu.gmall.realtime.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION;

public abstract class BaseApp {
    public abstract void handle(StreamExecutionEnvironment env,
                                DataStreamSource<String> stream);

    public void start(int port, int parallelism, String ckAndGroupId, String topic) {
        // 1. 环境准备
        // 1.1 设置操作 Hadoop 的用户名为 Hadoop 超级用户 atguigu
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 1.2 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.3 设置并行度
        /*
        env.setParallelism(parallelism);

        // 1.4 状态后端及检查点相关配置
        // 1.4.1 设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        // 1.4.2 开启 checkpoint
        env.enableCheckpointing(5000);
        // 1.4.3 设置 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 1.4.4 checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2023/stream/" + ckAndGroupId);
        // 1.4.5 checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 1.4.6 checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 1.4.7 checkpoint  的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        // 1.4.8 job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);
        */

        // 1.5 从 Kafka 目标主题读取数据，封装为流
        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(ckAndGroupId, topic);

        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 2. 执行具体的处理逻辑
        handle(env, stream);

        // 3. 执行 Job
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
