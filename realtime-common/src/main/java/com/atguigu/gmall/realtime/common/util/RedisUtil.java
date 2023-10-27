package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.constant.Constant;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

    private final static JedisPool pool;

    static {
        GenericObjectPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(300);
        config.setMaxIdle(10);
        config.setMinIdle(2);

        config.setTestOnCreate(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);

        config.setMaxWaitMillis(10 * 1000);

        pool = new JedisPool(config, "hadoop102", 6379);
    }

    public static Jedis getJedis() {
        // Jedis jedis = new Jedis("hadoop102", 6379);

        Jedis jedis = pool.getResource();
        jedis.select(4); // 直接选择 4 号库

        return jedis;
    }

    /**
     * 从 redis 读取维度数据
     *
     * @param jedis     jedis 对象
     * @param tableName 表名
     * @param id        维度的 id 值
     * @return 这条维度组成的 JSONObject 对象
     */
    public static JSONObject readDim(Jedis jedis, String tableName, String id) {
        String key = getKey(tableName, id);
        String jsonStr = jedis.get(key);
        if (jsonStr != null) {
            return JSON.parseObject(jsonStr);
        }
        return null;
    }

    public static void writeDim(Jedis jedis, String tableName, String id, JSONObject dim) {
        /*jedis.set(getKey(tableName, id), dim.toJSONString()); // 写入字符串
        jedis.expire(getKey(tableName, id), 2 * 24 * 60 * 60); // 设置过期时间*/

        jedis.setex(getKey(tableName, id), Constant.TWO_DAY_SECONDS, dim.toJSONString());
    }

    public static String getKey(String tableName, String id) {
        return tableName + ":" + id;
    }


    public static void closeJedis(Jedis jedis) {
        if (jedis != null) {
            jedis.close();  // 如果 jedis 客户端是 new Jedis()得到的,则是关闭客户端.如果是通过连接池得到的,则归还
        }
    }

    /**
     * 获取到 redis 的异步连接
     *
     * @return 异步链接对象
     */
    public static StatefulRedisConnection<String, String> getRedisAsyncConnection() {
        RedisClient redisClient = RedisClient.create("redis://hadoop102:6379/2");
        return redisClient.connect();
    }

    /**
     * 关闭 redis 的异步连接
     *
     * @param redisAsyncConn
     */
    public static void closeRedisAsyncConnection(StatefulRedisConnection<String, String> redisAsyncConn) {
        if (redisAsyncConn != null) {
            redisAsyncConn.close();
        }
    }

    /**
     * 异步的方式从 redis 读取维度数据
     * @param redisAsyncConn 异步连接
     * @param tableName 表名
     * @param id id 的值
     * @return 读取到维度数据,封装的 json 对象中
     */
    public static JSONObject readDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
                                          String tableName,
                                          String id) {
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();
        String key = getKey(tableName, id);
        try {
            String json = asyncCommand.get(key).get();
            if (json != null) {
                return JSON.parseObject(json);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    /**
     * 把维度异步的写入到 redis 中
     * @param redisAsyncConn  到 redis 的异步连接
     * @param tableName 表名
     * @param id id 的值
     * @param dim 要写入的维度数据
     */
    public static void writeDimAsync(StatefulRedisConnection<String, String> redisAsyncConn,
                                     String tableName,
                                     String id,
                                     JSONObject dim) {
        // 1. 得到异步命令
        RedisAsyncCommands<String, String> asyncCommand = redisAsyncConn.async();

        String key = getKey(tableName, id);
        // 2. 写入到 string 中: 顺便还设置的 ttl
        asyncCommand.setex(key, Constant.TWO_DAY_SECONDS, dim.toJSONString());

    }

}
