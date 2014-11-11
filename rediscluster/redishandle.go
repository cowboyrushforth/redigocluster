package rediscluster

import "github.com/garyburd/redigo/redis"
import "os"
import "time"
import "fmt"

type RedisHandle struct {
  Host  string 
  Port  string
  Pool *redis.Pool
}

// XXX: add some password protection
func NewRedisHandle(host string, port string, max_active int, debug bool) (*RedisHandle) {
  if debug {
    fmt.Println("[RedisHandle] Opening New Handle For Pid:", os.Getpid())
  }
  return &RedisHandle{Host: host,
                      Port: port,
                      Pool:  &redis.Pool{MaxIdle: 10,
                                         MaxActive: max_active,
                                         IdleTimeout: 30 * time.Second,
                                         Dial: func() (redis.Conn, error) {
                                          c, err := redis.Dial("tcp", host+":"+port)
                                          if err != nil {
                                            return nil, err
                                          }
                                          return c, nil
                                         }}}
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
