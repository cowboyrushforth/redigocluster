package rediscluster

import "github.com/garyburd/redigo/redis"
import "os"
import "time"

type RedisHandle struct {
	Host string
	Port string
	Pool *redis.Pool
}

type PoolConfig struct {
	MaxIdle     int
	MaxActive   int
	IdleTimeout time.Duration
	Password    string
	Database    int
}

// XXX: add some password protection - DONE
func NewRedisHandle(host string, port string, poolConfig PoolConfig, debug bool) *RedisHandle {
	if debug {
		log.Info("[RedisHandle] Opening New Handle For Pid:", os.Getpid())
	}

	return &RedisHandle{
		Host: host,
		Port: port,
		Pool: &redis.Pool{
			MaxIdle:     poolConfig.MaxIdle,
			MaxActive:   poolConfig.MaxActive,
			IdleTimeout: poolConfig.IdleTimeout,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", host+":"+port)
				if err != nil {
					return nil, err
				}
				if poolConfig.Password != "" {
					if _, err := c.Do("AUTH", poolConfig.Password); err != nil {
						c.Close()
						return nil, err
					}
				}
				if poolConfig.Database > 0 {
					if _, err := c.Do("SELECT", poolConfig.Database); err != nil {
						c.Close()
						return nil, err
					}
				}
				return c, nil
			},
		},
	}
}

func (self *RedisHandle) GetRedisConn() redis.Conn {
	rc := self.Pool.Get()
	for i := 0; i < 6; i++ {
		err := rc.Err()
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			rc = self.Pool.Get()
		} else {
			break
		}
	}
	return rc
}

func (self *RedisHandle) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	rc := self.GetRedisConn()
	defer rc.Close()
	return rc.Do(commandName, args...)
}

// XXX: is _not_ calling defer rc.Close()
//      so do it yourself later
func (self *RedisHandle) Send(cmd string, args ...interface{}) (err error) {
	rc := self.GetRedisConn()
	return rc.Send(cmd, args...)
}
