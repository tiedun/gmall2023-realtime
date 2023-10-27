package com.atguigu.gmall.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.util.HBaseUtil;
import com.atguigu.gmall.realtime.common.util.RedisUtil;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements DimFunction<T>{

    private StatefulRedisConnection<String, String> redisAsyncConn;
    private AsyncConnection hBaseAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        redisAsyncConn = RedisUtil.getRedisAsyncConnection();
        hBaseAsyncConn = HBaseUtil.getHBaseAsyncConnection();

    }

    @Override
    public void close() throws Exception {
        RedisUtil.closeRedisAsyncConnection(redisAsyncConn);
        HBaseUtil.closeAsyncHbaseConnection(hBaseAsyncConn);
    }

    // 异步调用, 流中每来一个元素, 这个方法执行一次
    // 参数 1: 需要异步处理的元素
    // 参数 2: 元素被异步处理完之后, 放入到ResultFuture中,则会输出到后面的流中
    @Override
    public void asyncInvoke(T bean,
                            ResultFuture<T> resultFuture) throws Exception {

        // jdb8 提供的专门用执行异步代码的框架
        // 1. 从 redis 去维度  第一个异步操作
        // 2. 从 hbase 去读维度 第二个异步操作
        // 3. 把维度补充到 bean 中

        CompletableFuture
                .supplyAsync(new Supplier<JSONObject>() {
                    @Override
                    public JSONObject get() {
                        // 1. 从 redis 去维度 . 把读取到的维度返回
                        return RedisUtil.readDimAsync(redisAsyncConn, getTableName(), getRowKey(bean));
                    }
                })
                .thenApplyAsync(new Function<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject apply(JSONObject dimFromRedis) {
                        //2. 当 redis 没有读到维度的时候,  从 hbase 读取维度数据
                        JSONObject dim = dimFromRedis;
                        if (dim == null) {  // redis 中没有读到
                            dim =  HBaseUtil.readDimAsync(hBaseAsyncConn, "gmall", getTableName(), getRowKey(bean));
                            // 把维度写入到 Redis 中
                            RedisUtil.writeDimAsync(redisAsyncConn, getTableName(), getRowKey(bean), dim);
                            log.info("走的是 hbase " + getTableName() + "  " + getRowKey(bean));

                        }else{
                            log.info("走的是 redis " + getTableName() + "  " + getRowKey(bean));
                        }

                        return dim;
                    }
                })
                .thenAccept(new Consumer<JSONObject>() {
                    @Override
                    public void accept(JSONObject dim) {
                        // 3. 补充维度到 bean 中.
                        addDims(bean, dim);
                        // 4. 把结果输出
                        resultFuture.complete(Collections.singleton(bean));
                    }
                });
    }
}
