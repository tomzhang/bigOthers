package jinke.com.redisOperate;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by tanghanzhuang on 2018/8/14
 */
public class RedisTest {
//连接实例的最大连接数
    private static int MAX_ACTIVE = 1024;
//控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
    private static int MAX_IDLE = 200;
    private static int MAX_WAIT = 10000;

    private static int TIMEOUT = 10000;

    private static boolean TEST_ON_BORROW = true;

    private static JedisPool jedisPool = null;

    public Jedis getJedis() {
        String ADDR = "172.16.11.250";
        int PORT = 6379;
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(MAX_ACTIVE);
        config.setMaxIdle(MAX_IDLE);
        config.setMaxWaitMillis(MAX_WAIT);
        config.setTestOnBorrow(TEST_ON_BORROW);
        jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT,"");
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }
    public static void returnResource(final Jedis jedis) {
        if(jedis != null) {
            jedis.close();
            jedisPool.close();
        }

    }

    public void testConnect() {
        Jedis jedis = getJedis();
        System.out.println(jedis.get("mission_tenant_account_100761036069"));

    }
}
