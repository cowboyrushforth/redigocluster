package rediscluster

import "strings"
import "strconv"
import "errors"
import "math/rand"
import "os"
import "fmt"

const RedisClusterHashSlots  = 16384
const RedisClusterRequestTTL = 16
const RedisClusterDefaultTimeout = 1

type RedisCluster struct {
  SeedHosts map[string]bool
  Handles  map[string]*RedisHandle        
  Slots    map[uint16]string
  RefreshTableASAP bool
  SingleRedisMode bool
  Debug bool
}

func NewRedisCluster(seed_redii []map[string]string, debug bool) RedisCluster {
  cluster := RedisCluster{RefreshTableASAP: false,
  SingleRedisMode: false,
  SeedHosts: make(map[string]bool),
  Handles: make(map[string]*RedisHandle),
  Slots: make(map[uint16]string),
  Debug: debug}

  if cluster.Debug {
    fmt.Println("[RedisCluster], PID", os.Getpid(), "StartingNewRedisCluster")
  }

  for _,redis := range seed_redii {
    for host, port := range redis {
      label := host+":"+port
      cluster.SeedHosts[label] = true
      cluster.Handles[label] = NewRedisHandle(host, port, debug)
    }
  }

  for addr,_ := range cluster.SeedHosts {
    node := cluster.addRedisHandleIfNeeded(addr)
    cluster_enabled := cluster.hasClusterEnabled(node)
    if cluster_enabled == false {
      if len(cluster.SeedHosts) == 1 {
        cluster.SingleRedisMode = true
      } else {
        panic(errors.New("Multiple Seed Hosts Given, But Cluster Support Disabled in Redis"))
      }
    }
  }

  if cluster.SingleRedisMode == false {
    cluster.populateSlotsCache()
  }
  return cluster
}

func (self *RedisCluster) hasClusterEnabled(node *RedisHandle) bool {
    _, err := node.Do("CLUSTER", "INFO")
    if err != nil {
      if err.Error() == "ERR This instance has cluster support disabled" ||
         err.Error() == "ERR unknown command 'CLUSTER'" {
           return false
      }
    }
    return true
}

// contact the startup nodes and try to fetch the hash slots -> instances
// map in order to initialize the Slots map.
func (self *RedisCluster) populateSlotsCache() {
  if self.SingleRedisMode == true {
    return
  }
  if self.Debug {
    fmt.Println("[RedisCluster], PID", os.Getpid(), "[PopulateSlots Running]")
  }
  for name,_ := range self.SeedHosts {
    if self.Debug {
      fmt.Println("[RedisCluster] [PopulateSlots] Checking: ", name)
    }
    node := self.addRedisHandleIfNeeded(name)
    cluster_info, err := node.Do("CLUSTER", "NODES")
    if err == nil {
      lines :=  strings.Split(string(cluster_info.([]uint8)), "\n")
      for _,line := range lines {
        if line != "" {
          fields := strings.Split(line, " ")
          addr := fields[1]
          if addr == ":0" {
            addr = name
          }
          // add to seedlist if not in cluster
          _, seedlist_exists := self.SeedHosts[addr]
          if !seedlist_exists {
            self.SeedHosts[addr] = true
          }
          // add to handles if not in handles
          self.addRedisHandleIfNeeded(name)

          slots := fields[8:len(fields)]
          for _, s_range := range slots {
            slot_range := s_range
            if slot_range != "[" {
              if self.Debug {
                fmt.Println("[RedisCluster] Considering Slot Range", slot_range)
              }
              r_pieces := strings.Split(slot_range,"-")  
              min,_ := strconv.Atoi(r_pieces[0])
              max,_ := strconv.Atoi(r_pieces[1])
              for i := min; i <= max; i++ {
                self.Slots[uint16(i)] = addr
              }
            } 
          }
        }
      }
      if self.Debug {
        fmt.Println("[RedisCluster] [Initializing] DONE, ", 
        "Slots: ", len(self.Slots), 
        "Handles So Far:", len(self.Handles), 
        "SeedList:", len(self.SeedHosts))
      }
      break
    }
  }
  self.switchToSingleModeIfNeeded()
}

func (self *RedisCluster) switchToSingleModeIfNeeded() {
  // catch case where we really intend to be on 
  // single redis mode, but redis was not
  // started on time
  if len(self.SeedHosts) == 1 && 
     len(self.Slots) == 0 && 
     len(self.Handles) == 1 {
    for _, node := range self.Handles {
      cluster_enabled := self.hasClusterEnabled(node)
      if cluster_enabled == false {
        self.SingleRedisMode = true
      }
    }
  }
}

func (self *RedisCluster) addRedisHandleIfNeeded(addr string) *RedisHandle {
  _, handle_exists := self.Handles[addr]
  if !handle_exists {
    pieces := strings.Split(addr, ":")
    self.Handles[addr] = NewRedisHandle(pieces[0], pieces[1], self.Debug)
  }
  return self.Handles[addr]
}

func (self *RedisCluster)  KeyForRequest(cmd string, args ...interface{}) string {
  cmd = strings.ToLower(cmd)
  if cmd == "info" ||     
  cmd == "multi" ||
  cmd == "exec" ||
  cmd == "slaveof" ||
  cmd == "config" ||
  cmd == "shutdown" {
    return ""
  }
  if args[0] == nil {
    return ""
  }
  strs := args[0].([]interface{})
  if strs != nil && strs[0] != nil {
    return strs[0].(string)
  }
  return ""
}

// Return the hash slot from the key.
func (self *RedisCluster) SlotForKey(key string) uint16 {
  checksum := ChecksumCRC16([]byte(key))
  slot := checksum % RedisClusterHashSlots
  return slot
}

func (self *RedisCluster) RandomRedisHandle() *RedisHandle {
  if len(self.Handles) == 0 {
    return nil
  }
  addrs := make([]string, len(self.Handles))
  i := 0
  for addr, _ := range self.Handles {
    addrs[i] = addr
    i++
  }
  rand_addrs := make([]string, i)
  perm := rand.Perm(i)
  for j, v := range perm {
    rand_addrs[v] = addrs[j]
  }
  handle := self.Handles[rand_addrs[0]]  
  self.switchToSingleModeIfNeeded()
  return handle
}

// Given a slot return the link (Redis instance) to the mapped node.
// Make sure to create a connection with the node if we don't have
// one.
func (self *RedisCluster) RedisHandleForSlot(slot uint16) *RedisHandle {

  node,exists := self.Slots[slot]
  // If we don't know what the mapping is, return a random node.
  if !exists {
    if self.Debug {
      fmt.Println("[RedisCluster] No One Appears Responsible For Slot: ", slot, "our slotsize is: ",len(self.Slots))
    }
    return self.RandomRedisHandle()
  }

  _,cx_exists := self.Handles[node]
  // add to cluster if not in cluster
  if !cx_exists {
    pieces := strings.Split(node, ":")
    self.Handles[node] = NewRedisHandle(pieces[0], pieces[1], self.Debug)
    // XXX consider returning random if failure
  }
  return self.Handles[node]
}

func (self *RedisCluster) disconnectAll() {
  if self.Debug {
    fmt.Println("[RedisCluster] PID:", os.Getpid()," [Disconnect!] Had Handles:", len(self.Handles))
  }
  // disconnect anyone in handles
  for _,handle := range self.Handles {
    handle.Pool.Close()
  }
  // nuke handles
  for addr,_ := range self.SeedHosts {
    delete(self.Handles, addr)
  }
  // nuke slots
  self.Slots = make(map[uint16]string)
}

func (self *RedisCluster) handleSingleMode(flush bool, cmd string, args ...interface{})  (reply interface{}, err error) {
  for _,handle := range self.Handles {
    if flush { 
      return handle.Do(cmd, args...)
    }
    return nil, handle.Send(cmd, args...)
  }
  return nil, errors.New("no redis handle found for single mode")
}

func (self *RedisCluster) SendClusterCommand(flush bool, cmd string, args ...interface{})  (reply interface{}, err error)  {

  // forward onto first redis in the handle
  // if we are set to single mode
  if self.SingleRedisMode == true {
    return self.handleSingleMode(flush, cmd, args...)
  }

  if self.RefreshTableASAP == true {
    if self.Debug {
      fmt.Println("[RedisCluster] Refresh Needed")
    }
    self.disconnectAll()
    self.populateSlotsCache()
    self.RefreshTableASAP = false
    // in case we realized we were now in Single Mode
    if self.SingleRedisMode == true {
      return self.handleSingleMode(flush, cmd, args...)
    }
  }

  ttl := RedisClusterRequestTTL
  key := self.KeyForRequest(cmd, args)
  try_random_node := false
  asking := false
  for  {
    if ttl <= 0 {
      break
    }
    ttl -= 1
    if key == "" {
      panic(errors.New("no way to dispatch this type of command to redis cluster"))
    }
    slot := self.SlotForKey(key)

    var redis *RedisHandle

    if self.Debug {
      fmt.Println("[RedisCluster] slot: ",slot, "key", key, "ttl", ttl)
    }

    if try_random_node {
      if self.Debug {
        fmt.Println("[RedisCluster] Trying Random Node")
      }
      redis = self.RandomRedisHandle()
      try_random_node = false
    } else {
      if self.Debug {
        fmt.Println("[RedisCluster] Trying Specific Node")
      }
      redis = self.RedisHandleForSlot(slot)
    }

    if redis == nil {
      if self.Debug {
        fmt.Println("[RedisCluster] could not get redis handle, bailing this round")
      }
      break
    }
    if self.Debug {
      fmt.Println("[RedisCluster] Got Host/Port: ", redis.Host, redis.Port)
    }

    if asking {
      if self.Debug { fmt.Println("ASKING") }
      redis.Send("ASKING") 
      asking = false
    }

    var err error
    var resp interface{}

    if flush { 
      resp, err = redis.Do(cmd, args...)
      if err == nil {
        if self.Debug { fmt.Println("[RedisCluster] Success") }
        return resp, nil
      }
    } else {
      err = redis.Send(cmd, args...)
      if err == nil {
        if self.Debug { fmt.Println("[RedisCluster] Success") }
        return nil, nil
      }
    }

    // ok we are here so err is not nil
    errv := strings.Split(err.Error(), " ")
    if errv[0] == "MOVED" || errv[0] == "ASK" {
      if errv[0] == "ASK" {
        if self.Debug { fmt.Println("[RedisCluster] ASK") }
        asking = true
      } else {
        // Serve replied with MOVED. It's better for us to
        // ask for CLUSTER NODES the next time.
        SetRefreshNeeded()
        newslot,_ := strconv.Atoi(errv[1])
        newaddr := errv[2]
        self.Slots[uint16(newslot)] = newaddr
        if self.Debug {
          fmt.Println("[RedisCluster] MOVED newaddr: ", newaddr, "new slot: ", newslot, "my slots len: ", len(self.Slots))
        }
      }
    } else {
      if self.Debug { fmt.Println("[RedisCluster] Other Error: ", err.Error()) }
      try_random_node = true
    }
  }
  if self.Debug {
    fmt.Println("[RedisCluster] Failed Command")
  }
  return nil, errors.New("could not complete command")
}


func (self *RedisCluster) Do(cmd string, args ...interface{}) (reply interface{}, err error) {
  return self.SendClusterCommand(true,cmd, args...) 
}

func (self *RedisCluster) Send(cmd string, args ...interface{}) (err error) {
  _, err = self.SendClusterCommand(false,cmd, args...) 
  return err
}

func (self *RedisCluster) SetRefreshNeeded() {
  self.RefreshTableASAP = true
}

func (self *RedisCluster) HandleForKey(key string) *RedisHandle {
  // forward onto first redis in the handle
  // if we are set to single mode
  if self.SingleRedisMode == true {
    for _,handle := range self.Handles {
      return handle
    }
  }
  slot := self.SlotForKey(key)
  handle := self.RedisHandleForSlot(slot)
  return handle
}

type RedisClusterAccess interface {
  Do(commandName string, args ...interface{}) (reply interface{}, err error) 
  Send(cmd string, args ...interface{}) (err error) 
  SetRefreshNeeded()
  HandleForKey(key string) *RedisHandle
}

var Instance RedisCluster

func Do(commandName string, args ...interface{}) (reply interface{}, err error) {
  return Instance.Do(commandName, args...) 
}

func Send(cmd string, args ...interface{}) (err error) {
  return Instance.Send(cmd, args...) 
}

func SetRefreshNeeded() {
  Instance.SetRefreshNeeded()
}

func HandleForKey(key string) *RedisHandle {
  return Instance.HandleForKey(key)
}
