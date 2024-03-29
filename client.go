package rpccluster

import (
	"encoding/gob"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// connection is a wrapper of net.Conn
type connection struct {
	conn net.Conn
	dec  *gob.Decoder
	enc  *gob.Encoder
}

func newConnection(tcpConn net.Conn) *connection {
	return &connection{
		conn: tcpConn,
		enc:  gob.NewEncoder(tcpConn),
		dec:  gob.NewDecoder(tcpConn),
	}
}

func (conn *connection) recv(data interface{}) error {
	return conn.dec.Decode(data)
}

func (conn *connection) send(data interface{}) error {
	return conn.enc.Encode(data)
}

// This type represents client connection.
type client struct {
	host     string
	uniqueID string
	// isMyself tells whether this connection is to localhost.
	// If it is the case, this package does not use network for the sake of performance.
	isMyself bool

	// mutex guard the following objects.
	mutex sync.Mutex
	// seq keeps track of ID of remote procedure call.
	seq int
	// pending is a map from ID to channel to receive a result.
	pending map[int](chan functionCallResponse)

	callReq chan functionCallRequest
}

func newClient(host string, uniqueID string) *client {
	cl := &client{
		host:     host,
		uniqueID: uniqueID,
		isMyself: false,
		seq:      0,
		pending:  make(map[int](chan functionCallResponse)),
		callReq:  make(chan functionCallRequest),
	}
	go cl.serve()
	return cl
}

func (cl *client) connect() (conn *connection, err error) {
	tcpConn, err := net.Dial("tcp", cl.host)
	if err != nil {
		return
	}
	conn = newConnection(tcpConn)
	cl.isMyself, err = checkIfMySelf(conn, cl.uniqueID)
	if err != nil {
		return
	}
	return
}

func (cl *client) mustConnect() *connection {
	retried := false
	for {
		conn, err := cl.connect()
		if err == nil {
			log.Printf("Connected to %s", cl.host)
			return conn
		}
		if !retried {
			log.Printf("Failed to connect to %s with error '%v'.  Retrying...", cl.host, err)
			retried = true
		}
		time.Sleep(time.Millisecond * 100)
	}
}

// receive UniqueID and check if it is equal to mine
func checkIfMySelf(conn *connection, myUniqueID string) (bool, error) {
	var uid string
	err := conn.recv(&uid)

	// banner check
	if strings.Split(uid, ":")[0] != RPCBanner {
		log.Fatalf("Unexpected banner in a cluster ID: %s", uid)
	}

	return uid == myUniqueID, err
}

func (cl *client) serve() {
	for {
		conn := cl.mustConnect()

		errChan := make(chan error, 2)
		stopChan := make(chan struct{})

		go cl.sender(conn, errChan, stopChan)
		go cl.receiver(conn, errChan)

		err := <-errChan // waiit until some error occurs
		close(stopChan)

		// broadcast the error to all the pending requests
		cl.mutex.Lock()
		for i, c := range cl.pending {
			var response functionCallResponse
			response.err = err
			response.ID = i
			c <- response
		}
		cl.mutex.Unlock()

		// clean up in order to reconnect
	}
}

func (cl *client) sender(conn *connection, errChan chan error, stopChan chan struct{}) {
	for {
		select {
		case <-stopChan:
			return
		case req := <-cl.callReq:
			err := conn.send(req)
			if err != nil {
				log.Printf("Error '%v' while sending to host %s\n", err, cl.host)
				errChan <- err
				return
			}
		}
	}
}

func (cl *client) receiver(conn *connection, errChan chan error) {
	for {
		var res functionCallResponse
		err := conn.recv(&res)
		if err != nil {
			log.Printf("Error '%v' while receiving from host %s\n", err, cl.host)
			errChan <- err
			return
		}
		i := res.ID
		cl.mutex.Lock()
		ch := cl.pending[i]
		cl.mutex.Unlock()
		ch <- res
	}
}

func (cl *client) call(funcName string, params ...interface{}) ([]interface{}, error) {
	var res []interface{}
	if cl.isMyself {
		// do not use network for performance
		return Call(funcName, params...), nil
	}
	ch := make(chan functionCallResponse, 2) // a channel might receive a result or an error
	cl.mutex.Lock()
	seq := cl.seq
	cl.seq += 1
	cl.pending[seq] = ch
	cl.mutex.Unlock()

	req := functionCallRequest{
		Name: funcName,
		Arg:  params,
		ID:   seq,
	}
	cl.callReq <- req
	response := <-ch
	if response.err != nil {
		return res, response.err
	}
	res = response.Results

	cl.mutex.Lock()
	delete(cl.pending, seq)
	cl.mutex.Unlock()
	return res, nil
}

// mustCall retries if an error occurs
func (cl *client) mustCall(funcName string, params ...interface{}) []interface{} {
	for {
		res, err := cl.call(funcName, params...)
		if err == nil {
			return res
		}
		log.Printf("Error '%v' in mustCall.  Retrying...\n", err)
	}
}
