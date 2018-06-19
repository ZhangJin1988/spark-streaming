package redis.op;

import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * @author zhangjin
 * @create 2018-06-14 17:33
 */
public class RedisApi {

    static final String HOST = "hdp1";


    @Test
    public void createKV() {

        Jedis jedis = new Jedis(HOST, 6379);

        jedis.set("zhangjin", "18");
        jedis.del("zhangjin");
        jedis.incrBy("a", 10);

        jedis.close();
    }


    @Test
    public void createhashes(){
        Jedis jedis = new Jedis(HOST, 6379);

        jedis.hset("qiannvyouhun","nicaicheng","20");
        jedis.hset("qiannvyouhun","yanchixia","40");


        jedis.expire("qiannvyouhun",10);
        jedis.close();

    }


    public static void main(String[] args) {

    }
}
