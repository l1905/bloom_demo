package org.example;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

// bloom test过滤方法
public class BloomTest {

    private static Logger log = LoggerFactory.getLogger(BloomTest.class);

    // 属性信息
    private Properties properties;

    // redis连接实例
    private JedisPool jedisInstance;

    // 最近10天的数据
    private int expire = 14;

    // 1个用户1天最多存储多少文章大小
    private int articleIdSize = 1000;

    // 待遍历的用户ID数
    private int userIdSize = 1; // 200
    // 第一个用户id值
    private int startUserId = 2000000;

    private int startArticleId = 21511071;

    private  int articlePoolSize = 2000;

    private int maxArticleSize = 14000;

    // 初始化配置， Redis, 连接
    public void initConfig(Properties newProperties) {
        properties = newProperties;

        System.out.println(properties.getProperty("jedis.bloom.maxIdle"));
        //Redis连接池初始化
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(Integer.parseInt(properties.getProperty("jedis.bloom.maxIdle")));
        jedisPoolConfig.setMaxTotal(Integer.parseInt(properties.getProperty("jedis.bloom.maxTotal")));
        jedisPoolConfig.setMaxWaitMillis(Integer.parseInt(properties.getProperty("jedis.bloom.maxWaitMillis")));
        jedisPoolConfig.setTestOnBorrow(Boolean.parseBoolean(properties.getProperty("jedis.bloom.testOnBorrow")));

        this.jedisInstance = new JedisPool(jedisPoolConfig, properties.getProperty("jedis.bloom.host"),
                Integer.parseInt(properties.getProperty("jedis.bloom.port")), 10000, properties.getProperty("jedis.bloom.password"));
    }

    // 方案1 和方案2 数据写入
    public void initData01() {
        // 记录开始时间
        TimeUtil timer0 = new TimeUtil();
        // 获取 Redis连接
        try (Jedis jedis = this.jedisInstance.getResource()) {
            // 计算需要获取最近几天的历史数据
            Date today = new Date();
            Date start = DateUtils.getDateBeforeOrAfterDays(today, -expire);
            List<String> days = DateUtils.getBetweenDays(start, today);
            log.info("打印需遍历的时间:{}", days);

            int loopArticle = startArticleId;
            // 初始化xxx个用户
            for(int i = 0; i< userIdSize; i++) {
                // 初始化bloomfilter对象
                BloomFilter<byte[]> bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), articleIdSize*expire, 0.001);
                // bloomfilter对象的redisKey
                String bloomKey = String.valueOf(startUserId+i) + "_"  + "history_show_ids_times_tj3_bloom";

                // 遍历xx天，初始化每天的用户历史访问数据
                for (int index = 0; index < days.size(); index++) {
                    // 使用Redis List存储的redisKey
                    String listKey = days.get(index) + "_"+ String.valueOf(startUserId+i) + "_"  + "history_show_ids_times_tj3_list";

                    Pipeline pipelinInstance = jedis.pipelined();
                    // 预设历史文章，文章ID分布 [loopArticle, loopArticle++]
                    for(int articleIndex =0; articleIndex < articleIdSize; articleIndex++) {

                        // 需要存入Redis中的值
                        String value = "3:"+String.valueOf(loopArticle++);
                        // 加入Redis List列表
                        pipelinInstance.lpush(listKey, value);
                        // 加入bloomfilter对象
                        bloomFilter.put(value.getBytes());
                    }
                    pipelinInstance.sync();
                }

                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                bloomFilter.writeTo(byteArrayOutputStream);
                jedis.set(bloomKey.getBytes(), byteArrayOutputStream.toByteArray());
                byteArrayOutputStream.close();

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 方案1 和方案2 数据对比
    public void check01() {
        // 获取 Redis连接
        try (Jedis jedis = this.jedisInstance.getResource()) {
            Date today = new Date();
            Date start = DateUtils.getDateBeforeOrAfterDays(today, -expire);
            List<String> days = DateUtils.getBetweenDays(start, today);
            log.info("打印需遍历的时间:{}", days);
            TimeUtil timer1 = new TimeUtil();
            // redis _list查询
            for(int i=0; i< userIdSize ; i++) {
                long listCount = 0;
                List<Set<String>> hasReadList = new ArrayList<>();
                for (int index = 0; index < days.size(); index++) {
                    // 列表key
                    String listKey = days.get(index) + "_"+ String.valueOf(startUserId+i) + "_"  + "history_show_ids_times_tj3_list";
                    // 获取已读列表
                    List<String> listData = jedis.lrange(listKey, 0, -1);
                    Set<String> setData = new HashSet<>(listData);
                    hasReadList.add(setData);
                    listCount = listCount + setData.size();
                }
//                log.info("list_count:{}", listCount);
                // 判断文章是否在列表中
                for(int articleIndex =startArticleId; articleIndex < startArticleId+articlePoolSize; articleIndex++) {
                    String value = "3:"+String.valueOf(articleIndex);
                    for (int j = 0; j < hasReadList.size() ; j++) {
                        if(hasReadList.get(j).contains(value)) {
//                            log.info("元素存在List");
                        }
                    }
                }
            }
            log.info("使用redis_list获取历史文章耗时:{}", timer1.getTimeAndReset());
            TimeUtil timer2 = new TimeUtil();
            // redis bloomfilter查询
            for(int i=0; i< userIdSize ; i++) {
                long bloomCount = 0;
                List<BloomFilter<byte[]>> hasReadBloom = new ArrayList<>();
                // 列表key
                String bloomKey = String.valueOf(startUserId+i) + "_"  + "history_show_ids_times_tj3_bloom";
                ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(jedis.get(bloomKey.getBytes()));
                BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(byteArrayOutputStream, Funnels.byteArrayFunnel());
                hasReadBloom.add(bloomFilter);
                bloomCount = bloomCount + bloomFilter.approximateElementCount();
//                log.info("bloom_count:{}", bloomCount);
                byteArrayOutputStream.close();
                // 文章ID
                for(int articleIndex =startArticleId; articleIndex < startArticleId+articlePoolSize; articleIndex++) {
                    String value = "3:"+String.valueOf(articleIndex);
                    for (int j = 0; j < hasReadBloom.size() ; j++) {
                        if(hasReadBloom.get(j).mightContain(value.getBytes())) {
//                            log.info("元素存在bloom");
                        }
                    }
                }
            }
            log.info("使用redis_bloomfilter获取历史文章耗时:{}", timer2.getTimeAndReset());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 方案1 和方案3 数据写入
    public void initData02() {
        // 记录开始时间
        TimeUtil timer0 = new TimeUtil();
        // 获取 Redis连接
        try (Jedis jedis = this.jedisInstance.getResource()) {
            // 计算需要获取最近几天的历史数据
            Date today = new Date();
            Date start = DateUtils.getDateBeforeOrAfterDays(today, -expire);
            List<String> days = DateUtils.getBetweenDays(start, today);
            log.info("打印需遍历的时间:{}", days);

            int loopArticle = startArticleId;
            // 初始化xxx个用户
            for(int i = 0; i< userIdSize; i++) {
                // 遍历xx天，初始化每天的用户历史访问数据
                for (int index = 0; index < days.size(); index++) {
                    // 初始化bloomfilter对象
                    BloomFilter<byte[]> bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), articleIdSize, 0.001);
                    // bloomfilter对象的redisKey
                    String bloomKey = days.get(index) + "_"+ String.valueOf(startUserId+i) + "_"  + "history_show_ids_times_tj3_bloom";

                    // 使用Redis List存储的redisKey
                    String listKey = days.get(index) + "_"+ String.valueOf(startUserId+i) + "_"  + "history_show_ids_times_tj3_list";

                    Pipeline pipelinInstance = jedis.pipelined();
                    // 预设历史文章，文章ID分布 [loopArticle, loopArticle++]
                    for(int articleIndex =0; articleIndex < articleIdSize; articleIndex++) {

                        // 需要存入Redis中的值
                        String value = "3:"+String.valueOf(loopArticle++);
                        // 加入Redis List列表
                        pipelinInstance.lpush(listKey, value);
                        // 加入bloomfilter对象
                        bloomFilter.put(value.getBytes());
                    }
                    pipelinInstance.sync();

                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    bloomFilter.writeTo(byteArrayOutputStream);
                    jedis.set(bloomKey.getBytes(), byteArrayOutputStream.toByteArray());
                    byteArrayOutputStream.close();
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 方案1 和方案3 数据对比
    public void check02() {
        // 获取 Redis连接
        try (Jedis jedis = this.jedisInstance.getResource()) {
            Date today = new Date();
            Date start = DateUtils.getDateBeforeOrAfterDays(today, -expire);
            List<String> days = DateUtils.getBetweenDays(start, today);
            log.info("打印需遍历的时间:{}", days);
            TimeUtil timer1 = new TimeUtil();

            // redis _list查询
            for(int i=0; i< userIdSize ; i++) {
                long listCount = 0;
                List<Set<String>> hasReadList = new ArrayList<>();
                for (int index = 0; index < days.size(); index++) {
                    // 列表key
                    String listKey = days.get(index) + "_"+ String.valueOf(startUserId+i) + "_"  + "history_show_ids_times_tj3_list";
                    // 获取已读列表
                    List<String> listData = jedis.lrange(listKey, 0, -1);
                    Set<String> setData = new HashSet<>(listData);
                    hasReadList.add(setData);
                    listCount = listCount + setData.size();
                }
//                log.info("list_count:{}", listCount);
                // 判断文章是否在列表中
                for(int articleIndex =startArticleId; articleIndex < startArticleId+articlePoolSize; articleIndex++) {
                    String value = "3:"+String.valueOf(articleIndex);
                    for (int j = 0; j < hasReadList.size() ; j++) {
                        if(hasReadList.get(j).contains(value)) {
//                            log.info("元素存在List");
                        }
                    }
                }
            }
            log.info("使用redis_list获取历史文章耗时:{}", timer1.getTimeAndReset());
            TimeUtil timer2 = new TimeUtil();
            // redis bloomfilter查询
            for(int i=0; i< userIdSize ; i++) {
                long bloomCount = 0;
                List<BloomFilter<byte[]>> hasReadBloom = new ArrayList<>();

                for (int index = 0; index < days.size(); index++) {
                    // 列表key
                    String bloomKey = days.get(index) + "_"+ String.valueOf(startUserId+i) + "_"  + "history_show_ids_times_tj3_bloom";
                    ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(jedis.get(bloomKey.getBytes()));
                    BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(byteArrayOutputStream, Funnels.byteArrayFunnel());
                    hasReadBloom.add(bloomFilter);
                    byteArrayOutputStream.close();
                    bloomCount = bloomCount + bloomFilter.approximateElementCount();
                }
//                log.info("bloom_count:{}", bloomCount);

                // 文章ID
                for(int articleIndex =startArticleId; articleIndex < startArticleId+articlePoolSize; articleIndex++) {
                    String value = "3:"+String.valueOf(articleIndex);
                    for (int j = 0; j < hasReadBloom.size() ; j++) {
                        if(hasReadBloom.get(j).mightContain(value.getBytes())) {
//                            log.info("元素存在bloom");
                        }
                    }
                }
            }
            log.info("使用redis_bloomfilter获取历史文章耗时:{}", timer2.getTimeAndReset());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 分片设计， 保留最近5000条浏览记录， 用户5天内没有新增浏览记录，则redis中删除，对应的key
    // 方案4 数据写入
    public void initData03() {
        int bloomFilterNum = 10;// 每个人允许布隆过滤器最大个数
        int maxKeyNum = 1000;  //单个布隆最多能存元素个数
        double falsePositives = 0.001;

        try (Jedis jedis = this.jedisInstance.getResource()) {
            // redis _list查询
            int loopArticle = startArticleId;
            for(int i=0; i< userIdSize ; i++) {
                // bloomkey 管理有具体多少个bloomSubKey
                String bloomKey = String.valueOf(startUserId+i) + "_"  + "history_show_ids_times_tj3_bloom";

                List<String> keyList = jedis.lrange(bloomKey, 0,0);
                // 如果没有对应的redisKey
                BloomFilter<byte[]> bloomFilter = null;
                BloomFilter<byte[]> defaultBloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), maxKeyNum, falsePositives);
                String statusTxt = "";
                String bloomSubKey = "";
                if(keyList.size() == 0) {
                    // 新增1个
                    bloomFilter = defaultBloomFilter;
                    statusTxt = "add";
                } else {
                    String subKey = keyList.get(0);
                    bloomSubKey = subKey;
                    byte[]  currentBloomData = jedis.get(subKey.getBytes());
                    if(currentBloomData != null) {
                        ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(currentBloomData);
                        bloomFilter = BloomFilter.readFrom(byteArrayOutputStream, Funnels.byteArrayFunnel());
                        // 查询容量是否已满
                        if(bloomFilter.approximateElementCount() >= maxKeyNum - 100) {
                            // 新增1个
                            bloomFilter = defaultBloomFilter;
                            statusTxt = "add";
                        } else {
                            statusTxt = "update";
                        }
                        // 其他情况下，修改此bloomfilter
                    } else {
                        // 新增1个
                        bloomFilter = defaultBloomFilter;
                        statusTxt = "add";
                    }
                }

                // 预设历史文章，文章ID分布 [loopArticle, loopArticle++]
                for(int articleIndex =0; articleIndex < maxArticleSize; articleIndex++) {
                    // 需要存入Redis中的值
                    String value = "3:"+String.valueOf(loopArticle++);
                    // 加入bloomfilter对象
                    bloomFilter.put(value.getBytes());

                    if(bloomFilter.approximateElementCount() >= maxKeyNum -100) {

                        // 容量已满，需要提交到redis, 并且生成新RedisKey
                        if(statusTxt == "update") {
                            // 设置1个元素
                            jedis.lset(bloomKey, 0, bloomSubKey);
                        } else {
                            double randNum = Math.random();
                            int randInt = (int)(randNum*100);
                            // 用户ID+当前毫秒数+随机数
                            bloomSubKey = String.valueOf(startUserId+i)+"_"+ String.valueOf(System.currentTimeMillis()) + String.valueOf(randInt);

                            // 调整长度
                            jedis.lpush(bloomKey, bloomSubKey);
                            jedis.ltrim(bloomKey, 0, bloomFilterNum -1);
                        }
                        // 设置bloomfilter对象
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        bloomFilter.writeTo(byteArrayOutputStream);
                        jedis.set(bloomSubKey.getBytes(), byteArrayOutputStream.toByteArray());
                        byteArrayOutputStream.close();

                        // 生成新的bloomfilter
                        bloomFilter = BloomFilter.create(Funnels.byteArrayFunnel(), maxKeyNum, 0.001);
                        // 重设状态
                        statusTxt = "add";
                    }


                }
                // 兜底处理
                if(bloomFilter.approximateElementCount() >= 0) {
                    if(statusTxt == "update") {
                        jedis.lset(bloomKey, 0, bloomSubKey);
                    } else {
                        double randNum = Math.random();
                        int randInt = (int)(randNum*100);
                        // 用户ID+当前毫秒数+随机数
                        bloomSubKey = String.valueOf(startUserId+i)+"_"+ String.valueOf(System.currentTimeMillis()) + String.valueOf(randInt);

                        // 调整长度
                        jedis.lpush(bloomKey, bloomSubKey);
                        jedis.ltrim(bloomKey, 0, bloomFilterNum-1);
                    }
                    // 设置bloomfilter对象
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    bloomFilter.writeTo(byteArrayOutputStream);
                    jedis.set(bloomSubKey.getBytes(), byteArrayOutputStream.toByteArray());
                    byteArrayOutputStream.close();
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 方案4 数据对比
    public void check03() {
        try (Jedis jedis = this.jedisInstance.getResource()) {
            TimeUtil timer1 = new TimeUtil();

            // redis bloomfilter查询
            for(int i=0; i< userIdSize ; i++) {
                long bloomCount = 0;
                List<BloomFilter<byte[]>> hasReadBloom = new ArrayList<>();

                String bloomKey = String.valueOf(startUserId+i) + "_"  + "history_show_ids_times_tj3_bloom";
                List<String> keyList = jedis.lrange(bloomKey, 0,-1);


                for (int index = 0; index < keyList.size(); index++) {
                    // 列表key
                    String bloomSubKey = keyList.get(index);
                    ByteArrayInputStream byteArrayOutputStream = new ByteArrayInputStream(jedis.get(bloomSubKey.getBytes()));
                    BloomFilter<byte[]> bloomFilter = BloomFilter.readFrom(byteArrayOutputStream, Funnels.byteArrayFunnel());
                    hasReadBloom.add(bloomFilter);
                    byteArrayOutputStream.close();
                    bloomCount = bloomCount + bloomFilter.approximateElementCount();
                }
//                log.info("bloom_count:{}", bloomCount);
                // 文章ID
                for(int articleIndex =startArticleId; articleIndex < startArticleId+articlePoolSize; articleIndex++) {
                    String value = "3:"+String.valueOf(articleIndex);
                    for (int j = 0; j < hasReadBloom.size() ; j++) {
                        if(hasReadBloom.get(j).mightContain(value.getBytes())) {
//                            log.info("元素存在bloom");
                        }
                    }
                }
            }
            log.info("check03耗费时间：{}", timer1.getTimeAndReset());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }






}
