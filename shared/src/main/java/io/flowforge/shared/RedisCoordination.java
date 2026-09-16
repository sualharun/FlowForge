package io.flowforge.shared;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

/** Advisory, short-lived optimizations only. Database locking guarantees correctness after Redis loss. */
@Component
public class RedisCoordination {
    private final StringRedisTemplate redis;
    private static final String LEASE="flowforge:scheduler:lease";
    private static final DefaultRedisScript<Long> RELEASE=new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",Long.class);
    public RedisCoordination(StringRedisTemplate redis) { this.redis=redis; }
    public boolean acquire(String token) {
        try { return Boolean.TRUE.equals(redis.opsForValue().setIfAbsent(LEASE,token,Duration.ofSeconds(5))); }
        catch (RuntimeException e) {
            LoggerFactory.getLogger(getClass()).debug("Redis unavailable; using PostgreSQL scheduler fence");return true;
        }
    }
    public void release(String token) {
        try { redis.execute(RELEASE,List.of(LEASE),token); }
        catch (RuntimeException e) { LoggerFactory.getLogger(getClass()).debug("Scheduler lease will expire"); }
    }
    public void cacheWorker(UUID workerId,int activeTasks) {
        try { redis.opsForValue().set("flowforge:worker:"+workerId,Integer.toString(activeTasks),Duration.ofSeconds(20)); }
        catch (RuntimeException e) { LoggerFactory.getLogger(getClass()).debug("Worker cache unavailable"); }
    }
}
