/*
Package 'rpccluster' enables RPC (remote procedure call) with easy interface.
Suppose that you have two servers, example1.com and example2.com, and you want to
share a sync.Map.  This package offers a solution with easy interface.
The following example illustrates a basic usage:
 import (
     "sync"
     "bitbucket.org/tailed/golang/rpccluster"
 )
 var m sync.Map
 // Construct a cluster which listens at 5000 port and connects to "example1.com:5000" and "example2.com:5000".
 var c *rpccluster.Cluster = rpccluster.NewCluster(5000, "example1.com:5000", "example2.com:5000")
 // Insert is a function which will be called remotely.
 func Insert(key string, value int) {
     m.Store(key, value)
 }
 func init() {
	 // Register a function to be called by RPC
     rpccluster.Register("Insert", Insert)
 }
 func main() {
	 // This RPC call insert a value in "example1.com" and "example2.com"
	 c.CallAll("Insert", "keyexample", 1234)
 }

You can also call a function with some return values.  For example,
suppose that you want to call a function which returns two values:
 func ReturnTwo(a int) (int, int) {
	 return a, a * 2
 }
You can call this function at example2.com from example1.com by running
the following code at example1.com.
 func init() {
	 rpccluster.Register("ReturnTwo", ReturnTwo)
 }
 func main() {
	 x := c.CallByID(1, "ReturnTwo", 1234)
	 fmt.Println(x[0].(int)) // 1234
	 fmt.Println(x[1].(int)) // 2468
 }
*/
package rpccluster

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"
)

const (
	RPCBanner string = "rpccluster"
)

func init() {
	log.SetPrefix("[rpccluster] ")
	log.SetFlags(log.Lshortfile)
}

type functionCallRequest struct {
	Name string
	Arg  []interface{}
	// if Async is true, do not return the result of a function call
	Async bool
	ID    int
}
type functionCallResponse struct {
	Results []interface{}
	ID      int
}

// Cluster is a server and clients.
// It is used for using remote procedure call as well as establishing a RPC server.
type Cluster struct {
	listener net.Listener

	uniqueID string
	myID     int

	// mutex protects clients
	mutex       sync.RWMutex
	connections []*client
	hostToID    map[string]int
}

// UniqueID is a server identification.
// It is just the concatenation of UnixNano time and hostname.
// If UniqueID is the same, then remote procedure call will directly invoke a function call without sending any data through network.
func (c *Cluster) UniqueID() string {
	return c.uniqueID
}

// MyID returns my own ID. (i.e. it is an index of the listed servers.)
// If there are multiple servers that are identified as myself, MyID returns the smallest ID among such servers.
// If there is no server identified as myself, MyID is -1.
// Note that this function should be called after connections are established.
func (c *Cluster) MyID() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	for id, cl := range c.connections {
		if cl.isMyself {
			return id
		}
	}
	return -1
}

func generateUniqueID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = ""
	}
	return fmt.Sprintf("%s:%d%s", RPCBanner, time.Now().UnixNano(), hostname)
}

// New creates empty cluster.  It is not listened nor connected to any server.
// You need to call Listen or ConnectTo in order to use Cluster.
func New() *Cluster {
	c := new(Cluster)
	c.connections = []*client{}
	c.uniqueID = generateUniqueID()
	c.hostToID = make(map[string]int)
	c.myID = -1
	return c
}

// NewCluster is a convenient constructor of Cluster.
// It blocks until the connection to all the servers succeeds.
// hosts must be specified by the "host:port" form.
// This function will block until it connects to all the server specified by hosts.
func NewCluster(listenPort int, hosts ...string) *Cluster {
	c := New()
	err := c.Listen(listenPort)
	if err != nil {
		log.Fatalf("failed to listen: %d\nError: %v", listenPort, err)
	}
	c.ConnectToAll(hosts...)
	return c
}

// Listen starts an RPC server at port listenPort.
func (c *Cluster) Listen(listenPort int) (err error) {
	c.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", listenPort))
	if err != nil {
		return
	}
	go c.serve()
	return
}

// ConnectTo host and register it as a peer identified by id.
// It returns the ID of the peer.
func (c *Cluster) ConnectTo(host string) int {
	client := newClient(host, c.uniqueID)
	return c.addClient(client)
}

// addClient adds a client to the connection list and return the ID of the client.
func (c *Cluster) addClient(client *client) int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	id := len(c.connections)
	c.connections = append(c.connections, client)
	c.hostToID[client.host] = id
	return id
}

// ConnectToAll connects to all the servers.
// The return values are IDs of servers.
func (c *Cluster) ConnectToAll(hosts ...string) []int {
	indices := []int{}
	for _, host := range hosts {
		indices = append(indices, c.ConnectTo(host))
	}
	return indices
}

// ReconnectAll establishes new connections to all servers.
func (c *Cluster) ReconnectAll() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, cl := range c.connections {
		c.connections[i] = newClient(cl.host, cl.uniqueID)
	}
}

func (c *Cluster) serve() {
	for {
		conn, err := c.listener.Accept()
		if err != nil {
			panic(err)
		}
		go c.handleConnection(newConnection(conn))
	}
}
func (c *Cluster) handleConnection(conn *connection) {
	if err := conn.send(c.uniqueID); err != nil {
		log.Println(err)
		return
	}
	cpus := runtime.NumCPU()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requestChannel := make(chan functionCallRequest, cpus)
	for i := 0; i < cpus; i++ {
		go func() {
			for {
				select {
				case req := <-requestChannel:
					res := Call(req.Name, req.Arg...)
					if !req.Async {
						conn.send(functionCallResponse{Results: res, ID: req.ID}) // ignore error
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	for {
		var req functionCallRequest
		err := conn.recv(&req)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Println(err)
			return
		}

		requestChannel <- req
	}
}

var funcMap = make(map[string]reflect.Value)

// Register registers a function to call over network.
// Every function that is called by RPC must be registered before its call.
func Register(name string, f interface{}) {
	if reflect.ValueOf(f).Kind() != reflect.Func {
		log.Panicf("In Register(%s, %v): Not a function", name, f)
	}
	funcMap[name] = reflect.ValueOf(f)
}

// Call a function named as "name" with argments params...
// Before calling this function, you need to first call Register.
func Call(name string, params ...interface{}) []interface{} {
	f, ok := funcMap[name]
	if !ok {
		log.Panicf("No such a function: %s", name)
	}
	argv := make([]reflect.Value, len(params))
	for i, param := range params {
		argv[i] = reflect.ValueOf(param)
	}
	resReflectValue := f.Call(argv)
	res := make([]interface{}, len(resReflectValue))
	for i, _ := range resReflectValue {
		res[i] = resReflectValue[i].Interface()
	}
	return res
}

// Call a function "funcName" at "host".
func (c *Cluster) Call(host string, funcName string, params ...interface{}) []interface{} {
	id, ok := c.hostToID[host]
	if !ok {
		log.Panicf("Unknown host: %s", host)
	}
	return c.CallByID(id, funcName, params...)
}
func (c *Cluster) CallAsync(host string, funcName string, params ...interface{}) []interface{} {
	id, ok := c.hostToID[host]
	if !ok {
		log.Panicf("Unknown host: %s", host)
	}
	return c.CallByIDAsync(id, funcName, params...)
}

// CallByID calls a function at the host identified by id.
// Its return value is a slice of interfaces of the returned values.
func (c *Cluster) CallByID(id int, funcName string, params ...interface{}) []interface{} {
	return c.connections[id].call(false, funcName, params...)
}
func (c *Cluster) CallByIDAsync(id int, funcName string, params ...interface{}) []interface{} {
	return c.connections[id].call(true, funcName, params...)
}

// CallAll calls the function "funcName" in all the connected servers.
func (c *Cluster) CallAll(funcName string, params ...interface{}) [][]interface{} {
	return c.callAll(false, funcName, params...)
}

func (c *Cluster) CallAllAsync(funcName string, params ...interface{}) [][]interface{} {
	return c.callAll(true, funcName, params...)
}

func (c *Cluster) callAll(async bool, funcName string, params ...interface{}) [][]interface{} {
	ids := make([]int, len(c.connections))
	for id := range ids {
		ids[id] = id
	}
	return c.CallByIDs(ids, async, funcName, params...)
}

// CallByIDs supports a fully general function call and specifies servers that are called, async or not, function name
func (c *Cluster) CallByIDs(ids []int, async bool, funcName string, params ...interface{}) [][]interface{} {
	var wg sync.WaitGroup
	var results = make([][]interface{}, len(c.connections))
	for _, id := range ids {
		if !async {
			wg.Add(1)
		}
		go func(id int) {
			results[id] = c.connections[id].call(async, funcName, params...)
			if !async {
				wg.Done()
			}
		}(id)
	}
	if !async {
		wg.Wait()
	}
	return results
}

// GobRegister registers your struct If error "panic: gob: type not registered for interface: YOUR STRUCT" happens.
// (Example:  cluster.GobRegister(MyStruct{}))
func GobRegister(d interface{}) {
	gob.Register(d)
}
