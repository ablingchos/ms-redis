package redisclient

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	metricNamespace = "ms"
)

var (
	MetricsRedisLabelNames = []string{
		"redis_method",
		"redis_status",
	}

	MRedisRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricNamespace,
		Name:      "redis_requests_total",
	}, MetricsRedisLabelNames)

	MRedisRequestLatencyMs = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricNamespace,
		Name:      "redis_request_latency_ms",
		Buckets:   []float64{0, 5, 10, 20, 40, 60, 80, 100, 500, 1000, 5000, 10000},
	}, MetricsRedisLabelNames)
)

// TODO (@kevin) 考虑进行错误注入，及相关测试

type RedisClient struct {
	client *redis.Client
	// reids连接配置参数
	opts *redis.Options
	// nameSpace 用于区分不同环境的前缀。
	// 正式的Key将是[nameSpace]_key.
	// 如果前缀为空，正式key就是其本身，不带下划线
	nameSpace string
}

type Script struct {
	src, namespace string
	entity         *redis.Script
}

type scripter interface {
	Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error)
	EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) (interface{}, error)
	ScriptExists(ctx context.Context, hashes ...string) (interface{}, error)
	ScriptLoad(ctx context.Context, script string) (interface{}, error)
}

type redisHook struct{}

type key string

const (
	startTimekey key = "startTime"
)

var _ redis.Hook = redisHook{}

func (redisHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	ctx = context.WithValue(ctx, startTimekey, time.Now())
	return ctx, nil
}

func (redisHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	startTime, _ := ctx.Value(startTimekey).(time.Time)
	elapsedTime := time.Since(startTime).Milliseconds()

	statusLabel := "OK"
	if cmd.Err() != nil && cmd.Err() != redis.Nil {
		statusLabel = cmd.Err().Error()
		// 因为有一些错误信息带了ip地址，会导致统计上有问题，这里特殊处理一下
		if strings.HasPrefix(statusLabel, "read") && strings.HasSuffix(statusLabel, "i/o timeout") {
			statusLabel = "read tcp: i/o timeout"
		}
	}
	labelValues := []string{
		cmd.Name(),
		statusLabel,
	}
	MRedisRequestsTotal.WithLabelValues(labelValues...).Inc()
	MRedisRequestLatencyMs.WithLabelValues(labelValues...).Observe(float64(elapsedTime))
	return nil
}

func (redisHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (redisHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	return nil
}

// NewRedisClient 创建redis客户端
// redis url, redis://user:passwd@redis-test:6379/?namespace=test
func NewRedisClient(uri string) (cli *RedisClient, e error) {
	opt, err := redis.ParseURL(uri)
	if err != nil {
		return nil, err
	}

	opt.DialTimeout = 2 * time.Second
	opt.MinIdleConns = 10

	c := RedisClient{}
	c.client = redis.NewClient(opt)
	c.client.AddHook(redisHook{})
	c.opts = opt
	c.nameSpace = opt.QueryStringValues.Get("namespace")

	// for test
	_, err = c.client.Ping().Result()
	if err != nil {
		err = fmt.Errorf("addr:%s err:%w", uri, err)
	} else {
		go func() {
			for {
				_, _ = c.client.Ping().Result()
				time.Sleep(5 * time.Second)
			}
		}()
	}

	return &c, err
}
func (c *RedisClient) NewScript(src string) *Script {
	entity := redis.NewScript(src)
	return &Script{
		src:       src,
		entity:    entity,
		namespace: c.nameSpace,
	}
}

func (c *RedisClient) GenKey(key string) string {
	if c.nameSpace == "" {
		return key
	}
	return fmt.Sprintf("@%s@%s", c.nameSpace, key)
}

func (s *Script) GenKey(key string) string {
	if s.namespace == "" {
		return key
	}
	return fmt.Sprintf("@%s@%s", s.namespace, key)
}

func (s *Script) MultiGenKey(keys *[]string) {
	for i := range *keys {
		(*keys)[i] = s.GenKey((*keys)[i])
	}
}

func (s *Script) Hash() string {
	return s.entity.Hash()
}

func (s *Script) Load(ctx context.Context, c scripter) (interface{}, error) {
	return c.ScriptLoad(ctx, s.src)
}

func (s *Script) Exists(ctx context.Context, c scripter) (interface{}, error) {
	return c.ScriptExists(ctx, s.Hash())
}

func (s *Script) Eval(ctx context.Context, c scripter, keys []string, args ...interface{}) (interface{}, error) {
	return c.Eval(ctx, s.src, keys, args...)
}

func (s *Script) EvalSha(ctx context.Context, c scripter, keys []string, args ...interface{}) (interface{}, error) {
	return c.EvalSha(ctx, s.entity.Hash(), keys, args...)
}

// Run optimistically uses EVALSHA to run the script. If script does not exist
// it is retried using EVAL.
func (s *Script) Run(ctx context.Context, c scripter, keys []string, args ...interface{}) (interface{}, error) {
	res, err := s.EvalSha(ctx, c, keys, args...)
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT ") {
		return s.Eval(ctx, c, keys, args...)
	}
	return res, err
}

// nolint
func (c *RedisClient) invoke(ctx context.Context, apiName string, cmd func() (interface{}, error)) (interface{}, error) {
	attrs := []attribute.KeyValue{semconv.DBSystemRedis}
	host, port, err := net.SplitHostPort(c.opts.Addr)
	if err == nil {
		attrs = append(attrs, semconv.NetPeerIPKey.String(host), semconv.NetPeerPortKey.String(port))
	}

	tracer := otel.Tracer("ms-go/redis")
	var span trace.Span
	_, span = tracer.Start(ctx, apiName, trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attrs...))
	defer span.End()

	ret, err := cmd()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return ret, err
}

func (c *RedisClient) Set(ctx context.Context, key, data string, expireSecond int) error {
	_, err := c.invoke(ctx, c.spanName("SET", key), func() (interface{}, error) {
		return nil, c.client.Set(c.GenKey(key), data, time.Duration(expireSecond)*time.Second).Err()
	})
	return err
}

func (c *RedisClient) Get(ctx context.Context, key string) (string, error) {
	val, err := c.invoke(ctx, c.spanName("GET", key), func() (interface{}, error) {
		val, err := c.client.Get(c.GenKey(key)).Result()
		if err == redis.Nil {
			val = ""
			err = nil
		} else if err != nil {
			return "", err
		}

		return val, err
	})

	return val.(string), err
}

func (c *RedisClient) MGet(ctx context.Context, keys ...string) ([]string, error) {
	val, err := c.invoke(ctx, c.spanName("MGET", keys...), func() (interface{}, error) {
		for i := range keys {
			keys[i] = c.GenKey(keys[i])
		}
		return c.client.MGet(keys...).Result()
	})
	if err != nil {
		return nil, err
	}

	val2 := val.([]interface{}) // nolint:errcheck
	var s []string
	for _, v := range val2 {
		if v == nil {
			s = append(s, "")
			continue
		}
		switch vv := v.(type) {
		case string:
			s = append(s, vv)
		case int64:
			s = append(s, strconv.FormatInt(vv, 10))
		}
	}
	return s, err
}

func (c *RedisClient) Del(ctx context.Context, key string) (bool, error) {
	val, err := c.invoke(ctx, c.spanName("DEL", key), func() (interface{}, error) {
		val, err := c.client.Del(c.GenKey(key)).Result()
		if err != nil {
			return false, err
		}
		if val == 0 {
			return false, nil
		}
		return true, nil
	})
	return val.(bool), err
}

func (c *RedisClient) Incr(ctx context.Context, key string, incr int64) (int64, error) {
	val, err := c.invoke(ctx, c.spanName("INCR", key), func() (interface{}, error) {
		return c.client.IncrBy(c.GenKey(key), incr).Result()
	})
	return val.(int64), err
}

func (c *RedisClient) SetNX(ctx context.Context, key, data string, expireSecond int) (bool, error) {
	val, err := c.invoke(ctx, c.spanName("SETNX", key), func() (interface{}, error) {
		return c.client.SetNX(c.GenKey(key), data, time.Duration(expireSecond)*time.Second).Result()
	})
	return val.(bool), err
}

func (c *RedisClient) SetXX(ctx context.Context, key, data string, expireSecond int) (bool, error) {
	val, err := c.invoke(ctx, c.spanName("SETXX", key), func() (interface{}, error) {
		return c.client.SetXX(c.GenKey(key), data, time.Duration(expireSecond)*time.Second).Result()
	})
	return val.(bool), err
}

func (c *RedisClient) HGet(ctx context.Context, key, field string) (string, error) {
	val, err := c.invoke(ctx, c.spanName("HGET", key), func() (interface{}, error) {
		val, err := c.client.HGet(c.GenKey(key), field).Result()
		if err == redis.Nil {
			val = ""
			err = nil
		} else if err != nil {
			return "", err
		}
		return val, err
	})
	return val.(string), err
}

func (c *RedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	val, err := c.invoke(ctx, c.spanName("HGETALL", key), func() (interface{}, error) {
		return c.client.HGetAll(c.GenKey(key)).Result()
	})

	return val.(map[string]string), err
}

func (c *RedisClient) HDel(ctx context.Context, key string, fields ...string) error {
	_, err := c.invoke(ctx, c.spanName("HDEL", key), func() (interface{}, error) {
		return c.client.HDel(c.GenKey(key), fields...).Result()
	})
	return err
}

func (c *RedisClient) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	val, err := c.invoke(ctx, c.spanName("HINCRBY", key), func() (interface{}, error) {
		return c.client.HIncrBy(c.GenKey(key), field, incr).Result()
	})
	return val.(int64), err
}

func (c *RedisClient) HMGet(ctx context.Context, key string, fields ...string) ([]string, error) {
	val, err := c.invoke(ctx, c.spanName("HMGET", key), func() (interface{}, error) {
		return c.client.HMGet(c.GenKey(key), fields...).Result()
	})
	if err != nil {
		return nil, err
	}

	val2 := val.([]interface{}) // nolint:errcheck
	var s []string
	for _, v := range val2 {
		if v == nil {
			s = append(s, "")
			continue
		}
		switch vv := v.(type) {
		case string:
			s = append(s, vv)
		case int64:
			s = append(s, strconv.FormatInt(vv, 10))
		}
	}
	return s, err
}

func (c *RedisClient) HMSet(ctx context.Context, key string, fields map[string]interface{}) error {
	_, err := c.invoke(ctx, c.spanName("HMSET", key), func() (interface{}, error) {
		return nil, c.client.HMSet(c.GenKey(key), fields).Err()
	})
	return err
}

func (c *RedisClient) HSet(ctx context.Context, key, field string, value interface{}) error {
	_, err := c.invoke(ctx, c.spanName("HSET", key), func() (interface{}, error) {
		return nil, c.client.HSet(c.GenKey(key), field, value).Err()
	})
	return err
}

// CAS 这是由tredis提供的CAS乐观锁。如果使用社区版本redis不要调用此接口
func (c *RedisClient) CAS(ctx context.Context, key string, version int64, value string) error {
	_, err := c.invoke(ctx, c.spanName("CAS", key), func() (interface{}, error) {
		return nil, c.client.CAS(c.GenKey(key), version, value).Err()
	})
	return err
}

func (c *RedisClient) GetVSN(ctx context.Context, key string) (int64, string, error) {
	rets, err := c.invoke(ctx, c.spanName("GETVSN", key), func() (interface{}, error) {
		ver, val, err := c.client.GetVSN(c.GenKey(key)).Result()
		if err != nil {
			return nil, err
		}
		rets := make([]interface{}, 2)
		rets[0] = ver
		rets[1] = val
		return rets, err
	})

	if err != nil {
		return -1, "", err
	}

	if rets, ok := rets.([]interface{}); ok {
		return rets[0].(int64), rets[1].(string), err
	}
	return -1, "", fmt.Errorf("do GetVSN return not []interface{}")
}

func (c *RedisClient) spanName(cmd string, keys ...string) string {
	var parts []string
	parts = append(parts, cmd)
	for _, key := range keys {
		parts = append(parts, c.GenKey(key))
	}
	return strings.Join(parts, " ")
}

func (c *RedisClient) LPush(ctx context.Context, key string, values ...interface{}) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("LPUSH", key), func() (interface{}, error) {
		return c.client.LPush(c.GenKey(key), values...).Result()
	})
	return ret, err
}

func (c *RedisClient) RPush(ctx context.Context, key string, values ...interface{}) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("RPUSH", key), func() (interface{}, error) {
		return c.client.RPush(c.GenKey(key), values...).Result()
	})
	return ret, err
}

func (c *RedisClient) LPop(ctx context.Context, key string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("LPOP", key), func() (interface{}, error) {
		return c.client.LPop(c.GenKey(key)).Result()
	})
	return ret, err
}

func (c *RedisClient) RPop(ctx context.Context, key string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("RPOP", key), func() (interface{}, error) {
		return c.client.RPop(c.GenKey(key)).Result()
	})
	return ret, err
}

func (c *RedisClient) LLen(ctx context.Context, key string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("LLEN", key), func() (interface{}, error) {
		return c.client.LLen(c.GenKey(key)).Result()
	})
	return ret, err
}

func (c *RedisClient) LTrim(ctx context.Context, key string, start, stop int64) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("LTRIM", key), func() (interface{}, error) {
		return c.client.LTrim(c.GenKey(key), start, stop).Result()
	})
	return ret, err
}

func (c *RedisClient) LRange(ctx context.Context, key string, start, stop int64) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("LRANGE", key), func() (interface{}, error) {
		return c.client.LRange(c.GenKey(key), start, stop).Result()
	})
	return ret, err
}

func (c *RedisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("Eval", keys...), func() (interface{}, error) {
		genKeys := make([]string, len(keys))
		for i := range keys {
			genKeys[i] = c.GenKey(keys[i])
		}
		return c.client.Eval(script, genKeys, args...).Result()
	})
	return ret, err
}

func (c *RedisClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("Eval", keys...), func() (interface{}, error) {
		genKeys := make([]string, len(keys))
		for i := range keys {
			genKeys[i] = c.GenKey(keys[i])
		}
		return c.client.EvalSha(sha1, genKeys, args...).Result()
	})
	return ret, err
}

func (c *RedisClient) ScriptLoad(ctx context.Context, script string) (interface{}, error) {
	return c.client.ScriptLoad(script).Result()
}

func (c *RedisClient) ScriptExists(ctx context.Context, hashes ...string) (interface{}, error) {
	return c.client.ScriptExists(hashes...).Result()
}

type Z = redis.Z

func (c *RedisClient) ZAdd(ctx context.Context, key string, members ...*Z) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZADD", key), func() (interface{}, error) {
		return c.client.ZAdd(c.GenKey(key), members...).Result()
	})
	return ret, err
}

func (c *RedisClient) ZRange(ctx context.Context, key string, start, stop int64) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZRANGE", key), func() (interface{}, error) {
		return c.client.ZRange(c.GenKey(key), start, stop).Result()
	})
	return ret, err
}

func (c *RedisClient) ZRevRange(ctx context.Context, key string, start, stop int64) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZREVRANGE", key), func() (interface{}, error) {
		return c.client.ZRevRange(c.GenKey(key), start, stop).Result()
	})
	return ret, err
}

func (c *RedisClient) ZRangeWithScores(ctx context.Context, key string, start, stop int64) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZRANGE", key), func() (interface{}, error) {
		return c.client.ZRangeWithScores(c.GenKey(key), start, stop).Result()
	})
	return ret, err
}

func (c *RedisClient) ZRangeByScore(ctx context.Context, key string, min, max string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZRANGEBYSCORE", key), func() (interface{}, error) {
		return c.client.ZRangeByScore(c.GenKey(key), &redis.ZRangeBy{
			Min: min,
			Max: max,
		}).Result()
	})
	return ret, err
}

func (c *RedisClient) ZCard(ctx context.Context, key string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZCARD", key), func() (interface{}, error) {
		return c.client.ZCard(c.GenKey(key)).Result()
	})
	return ret, err
}

func (c *RedisClient) ZCount(ctx context.Context, key string, min, max string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZCOUNT", key), func() (interface{}, error) {
		return c.client.ZCount(c.GenKey(key), min, max).Result()
	})
	return ret, err
}

func (c *RedisClient) ZRank(ctx context.Context, key, member string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZRANK", key), func() (interface{}, error) {
		return c.client.ZRank(c.GenKey(key), member).Result()
	})
	return ret, err
}

func (c *RedisClient) ZRevRank(ctx context.Context, key, member string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZREVRANK", key), func() (interface{}, error) {
		return c.client.ZRevRank(c.GenKey(key), member).Result()
	})
	return ret, err
}

func (c *RedisClient) ZRem(ctx context.Context, key string, members ...interface{}) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZREM", key), func() (interface{}, error) {
		return c.client.ZRem(c.GenKey(key), members...).Result()
	})
	return ret, err
}

func (c *RedisClient) ZPopMax(ctx context.Context, key string, count ...int64) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZPOPMAX", key), func() (interface{}, error) {
		return c.client.ZPopMax(c.GenKey(key), count...).Result()
	})
	return ret, err
}

func (c *RedisClient) ZPopMin(ctx context.Context, key string, count ...int64) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZPOPMIN", key), func() (interface{}, error) {
		return c.client.ZPopMin(c.GenKey(key), count...).Result()
	})
	return ret, err
}

func (c *RedisClient) ZScore(ctx context.Context, key, member string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZSCORE", key), func() (interface{}, error) {
		return c.client.ZScore(c.GenKey(key), member).Result()
	})
	return ret, err
}

func (c *RedisClient) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZREMRANGEBYRANK", key), func() (interface{}, error) {
		return c.client.ZRemRangeByRank(c.GenKey(key), start, stop).Result()
	})
	return ret, err
}

func (c *RedisClient) ZRemRangeByScore(ctx context.Context, key, min, max string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZREMRANGEBYSCORE", key), func() (interface{}, error) {
		return c.client.ZRemRangeByScore(c.GenKey(key), min, max).Result()
	})
	return ret, err
}

func (c *RedisClient) ZIncrBy(ctx context.Context, key string, incr float64, member string) (interface{}, error) {
	ret, err := c.invoke(ctx, c.spanName("ZINCRBY", key), func() (interface{}, error) {
		return c.client.ZIncrBy(c.GenKey(key), incr, member).Result()
	})
	return ret, err
}
