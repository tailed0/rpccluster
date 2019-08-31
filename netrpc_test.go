package rpccluster

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"testing"
)

type Args struct {
	A int
	B string
}

type Arith int

func (t *Arith) Multiply(args *Args, reply *string) error {
	*reply = fmt.Sprintf("%s: %d * 2 = %d", args.B, args.A, args.A*2)
	return nil
}

func init() {
	// start RPC server
	arith := new(Arith)
	rpc.Register(arith)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":23456")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func BenchmarkRPC(b *testing.B) {
	client, err := rpc.DialHTTP("tcp", "localhost:23456")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(100)
		args := &Args{x, "hello"}
		var reply string
		err = client.Call("Arith.Multiply", args, &reply)
		if err != nil {
			log.Fatal("arith error:", err)
		}
	}
}
