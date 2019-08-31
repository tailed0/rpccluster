package rpccluster

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func startLocalServers(ports []int) []*Cluster {
	var hosts = make([]string, len(ports))
	for i, port := range ports {
		hosts[i] = fmt.Sprintf("localhost:%d", port)
	}

	var cs = make([]*Cluster, len(ports))
	var wg sync.WaitGroup
	defer wg.Wait()
	for i, _ := range ports {
		wg.Add(1)
		go func(i int) {
			port := ports[i]
			cs[i] = NewCluster(port, hosts...)
			wg.Done()
		}(i)
	}
	return cs
}

func MyFunc(a int, b string) string {
	return fmt.Sprintf("%s: %d * 2 = %d", b, a, a*2)
}

func TestCluster(t *testing.T) {
	Register("MyFunc", MyFunc)

	rand.Seed(time.Now().UnixNano())
	numberOfServer := 4
	ports := []int{}
	for i := 0; i < numberOfServer; i++ {
		ports = append(ports, 20000+i)
	}
	cs := startLocalServers(ports)

	c := cs[0]
	results := c.CallAll("MyFunc", 2, "hello")
	for _, res := range results {
		if res[0].(string) != MyFunc(2, "hello") {
			t.Fatal("unexpected")
		}
	}
}

func DoubleI(a int) int {
	return a * 2
}
func DoubleS(a string) string {
	return a + a
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
func TestConcurrency(t *testing.T) {
	port1 := 20000 + rand.Intn(10000)
	port2 := 20000 + rand.Intn(10000)
	cs := startLocalServers([]int{port1, port2})
	Register("DoubleI", DoubleI)
	Register("DoubleS", DoubleS)
	var wg sync.WaitGroup
	n := 1000
	for th := range []int{1, 2, 3, 4} {
		wg.Add(1)
		go func(th int) {
			for i := 0; i < n; i++ {
				if th%2 == 0 {
					a := i //rand.Intn(1000)
					res := cs[0].CallByID(1, "DoubleI", a)
					b := res[0].(int)
					if a*2 != b {
						t.Fatalf("%d * 2 != %d", a, b)
					}
				} else {
					a := "hello"
					res := cs[0].CallByID(1, "DoubleS", a)
					b := res[0].(string)
					if a+a != b {
						t.Fatalf("%s + %s != %s", a, a, b)
					}
				}
			}
			wg.Done()
		}(th)
	}
	wg.Wait()
}

func BenchmarkSimpleCall(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(100)
		MyFunc(x, "hello")
	}
}

func BenchmarkLocalCall(b *testing.B) {
	port1 := 20000 + rand.Intn(10000)
	port2 := 20000 + rand.Intn(10000)
	cs := startLocalServers([]int{port1, port2})
	Register("MyFunc", MyFunc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(100)
		cs[0].CallByID(0, "MyFunc", x, "hello")
	}
}

func BenchmarkRemoteCall(b *testing.B) {
	port1 := 20000 + rand.Intn(10000)
	port2 := 20000 + rand.Intn(10000)
	cs := startLocalServers([]int{port1, port2})
	Register("MyFunc", MyFunc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(100)
		cs[0].CallByID(1, "MyFunc", x, "hello")
	}
}

func BenchmarkCallAll(b *testing.B) {
	port1 := 20000 + rand.Intn(10000)
	port2 := 20000 + rand.Intn(10000)
	cs := startLocalServers([]int{port1, port2})
	Register("MyFunc", MyFunc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		x := rand.Intn(100)
		cs[0].CallAll("MyFunc", x, "hello")
	}
}

func TestCheckSendBlock(t *testing.T) {
	// note that tcp send func may block...
	port1 := 20000 + rand.Intn(10000)
	port2 := 20000 + rand.Intn(10000)
	cs := startLocalServers([]int{port1, port2})
	var wg sync.WaitGroup
	n := 50000
	Register("MyFunc", MyFunc)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			x := rand.Intn(100)
			cs[0].CallByID(1, "MyFunc", x, "hello")
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func SlowFunc() {
	time.Sleep(time.Millisecond * 100)
}
func BenchmarkCallSlowFunc(b *testing.B) {
	port1 := 20000 + rand.Intn(10000)
	port2 := 20000 + rand.Intn(10000)
	cs := startLocalServers([]int{port1, port2})
	Register("SlowFunc", SlowFunc)
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			cs[0].CallAll("SlowFunc")
			wg.Done()
		}(i)
	}
	wg.Wait()
}

type MyStruct struct {
	ID   int
	Name string
}

func ShowMyStruct(s MyStruct) string {
	return fmt.Sprintf("%d %s", s.ID, s.Name)
}
func TestMyStruct(t *testing.T) {
	port1 := 20000 + rand.Intn(10000)
	port2 := 20000 + rand.Intn(10000)
	cs := startLocalServers([]int{port1, port2})
	GobRegister(MyStruct{})
	Register("ShowMyStruct", ShowMyStruct)
	cs[0].CallAll("ShowMyStruct", MyStruct{3, "hoge"})
}

/* pointer function call does not work (why?) */
/*
func ShowMyStruct2(s *MyStruct) string {
	return fmt.Sprintf("%d %s", s.ID, s.Name)
}
func TestMyStruct2(t *testing.T) {
	port1 := 20000 + rand.Intn(10000)
	port2 := 20000 + rand.Intn(10000)
	cs := startLocalServers([]int{port1, port2})
	Register("ShowMyStruct2", ShowMyStruct2)
	ShowMyStruct2(&MyStruct{3, "hoge"})
	cs[0].CallAll("ShowMyStruct2", &MyStruct{3, "hoge"})
}
*/

func TestDisconnected(t *testing.T) {
	Register("MyFunc", MyFunc)
	port1 := 20000 + rand.Intn(10000)
	port2 := 20000 + rand.Intn(10000)
	cs := startLocalServers([]int{port1, port2})
	cs[0].ReconnectAll()
	cs[1].ReconnectAll()
	//	cs[0].MustReconnectToAll()
	res := cs[0].CallByID(1, "MyFunc", 2, "hello")
	if res[0].(string) != MyFunc(2, "hello") {
		t.Fatal("unexpected")
	}
}

func TestMyID(t *testing.T) {
	port1 := 20000 + rand.Intn(10000)
	port2 := 20000 + rand.Intn(10000)
	cs := startLocalServers([]int{port1, port2})
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < 2; i++ {
		if cs[i].MyID() != i {
			t.Fatal("ID is not expected")
		}
	}
}
