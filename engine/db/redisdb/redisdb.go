package redisdb

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

type ConfigRedis struct {
	IP          string
	Port        int
	Password    string
	DbIndex     int
	MaxIdle     int //最大的空闲连接数，表示即使没有redis连接时依然可以保持N个空闲的连接，而不被清除，随时处于待命状态。
	MaxActive   int //最大的激活连接数，表示同时最多有N个连接
	IdleTimeout int //最大的空闲连接等待时间，超过此时间后，空闲连接将被关闭
}

type RedisDB struct {
	config    *ConfigRedis
	redisPool *redis.Pool
}

func NewRedisDB(config *ConfigRedis) *RedisDB {
	redisDB := &RedisDB{
		config: config,
		redisPool: &redis.Pool{
			Wait:        true,
			MaxIdle:     config.MaxIdle,
			MaxActive:   config.MaxActive,
			IdleTimeout: time.Duration(config.IdleTimeout) * time.Second,
			Dial: func() (redis.Conn, error) {
				opt := []redis.DialOption{redis.DialDatabase(config.DbIndex)}
				if config.Password != "" {
					opt = append(opt, redis.DialPassword(config.Password))
				}

				redisServer := fmt.Sprintf("%s:%d", config.IP, config.Port)
				c, err := redis.Dial("tcp", redisServer, opt...)
				if err != nil {
					return nil, err
				}

				return c, err
			},

			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				if time.Since(t) < time.Minute {
					return nil
				}
				_, err := c.Do("PING")
				if err != nil {
					return err
				}
				return err
			},
		},
	}
	return redisDB
}

func (db *RedisDB) Ping() error {
	conn := db.redisPool.Get()
	err := db.redisPool.TestOnBorrow(conn, time.Now())
	if err != nil {
		return err
	}

	defer conn.Close()

	return nil
}

func (db *RedisDB) Get(key string) (interface{}, error) {
	conn := db.redisPool.Get()
	if conn.Err() != nil {
		return nil, conn.Err()
	}

	defer conn.Close()

	return conn.Do("GET", key)
}

func (db *RedisDB) Set(key string, value interface{}, expire int) error {
	conn := db.redisPool.Get()
	if conn.Err() != nil {
		return conn.Err()
	}

	defer conn.Close()

	if expire <= 0 {
		_, err := conn.Do("SET", key, value)
		return err
	}
	_, err := conn.Do("SET", key, value, "EX", expire)
	return err
}

func (db *RedisDB) Del(key string) error {
	conn := db.redisPool.Get()
	if conn.Err() != nil {
		return conn.Err()
	}

	defer conn.Close()

	_, err := conn.Do("DEL", key)
	return err
}

func (db *RedisDB) Incr(key string) (int64, error) {
	conn := db.redisPool.Get()
	if conn.Err() != nil {
		return 0, conn.Err()
	}

	defer conn.Close()

	return redis.Int64(conn.Do("INCR", key))
}

func (db *RedisDB) Decr(key string) (int64, error) {
	conn := db.redisPool.Get()
	if conn.Err() != nil {
		return 0, conn.Err()
	}

	defer conn.Close()

	return redis.Int64(conn.Do("DECR", key))
}

func (db *RedisDB) HGet(key, field string) (interface{}, error) {
	conn := db.redisPool.Get()
	if conn.Err() != nil {
		return nil, conn.Err()
	}

	defer conn.Close()

	return conn.Do("HGET", key, field)
}

func (db *RedisDB) HSet(key, field string, value interface{}) error {
	conn := db.redisPool.Get()
	if conn.Err() != nil {
		return conn.Err()
	}

	defer conn.Close()

	_, err := conn.Do("HSET", key, field, value)
	return err
}

func (db *RedisDB) HDel(key, field string) error {
	conn := db.redisPool.Get()
	if conn.Err() != nil {
		return conn.Err()
	}

	defer conn.Close()

	_, err := conn.Do("HDEL", key, field)
	return err
}

func (db *RedisDB) HMGet(key string, fields ...string) ([]interface{}, error) {
	conn := db.redisPool.Get()
	if conn.Err() != nil {
		return nil, conn.Err()
	}

	defer conn.Close()

	args := make([]interface{}, len(fields)+1)
	args[0] = key
	for i, field := range fields {
		args[i+1] = field
	}
	return redis.Values(conn.Do("HMGET", args...))
}

func (db *RedisDB) HMSet(key string, fields map[string]interface{}) error {
	conn := db.redisPool.Get()
	if conn.Err() != nil {
		return conn.Err()
	}

	defer conn.Close()

	args := make([]interface{}, len(fields)*2+1)
	args[0] = key
	i := 1
	for field, value := range fields {
		args[i] = field
		args[i+1] = value
		i += 2
	}
	_, err := conn.Do("HMSET", args...)
	return err
}

