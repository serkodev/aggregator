package aggregator

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

var aDatas sync.Map
var caDatas sync.Map

var a = NewAggregator(func(ids []string, args ...any) (map[string]string, error) {
	fmt.Printf("a process: %v", ids)

	if args != nil {
		if sleepT, ok := args[0].(time.Duration); ok {
			time.Sleep(sleepT)
		}
	}

	out := make(map[string]string)
	for _, id := range ids {
		if r, ok := aDatas.Load(id); ok {
			out[id] = r.(string)

			// save cache
			caDatas.Store(id, r)
		} else {
			out[id] = ""
		}
	}

	return out, nil
}, 3000*time.Millisecond, 3600)

var ca = NewAggregator(func(ids []string, args ...any) (map[string]string, error) {
	fmt.Printf("ca Process: %v", ids)

	if args != nil {
		if sleepT, ok := args[0].(time.Duration); ok {
			time.Sleep(sleepT)
		}
	}

	out := make(map[string]string)
	// noCache := make([]string, 0, len(ids))

	for _, id := range ids {
		if r, ok := caDatas.Load(id); ok {
			out[id] = r.(string)
		} else {
			// out[id] = a.QueryChan(id)
			// noCache = append(noCache, id)
		}
	}

	if args != nil {
		if sleepT, ok := args[0].(time.Duration); ok {
			time.Sleep(sleepT)
		}
	}

	return out, nil
}, 20*time.Millisecond, 3600)

var alist = NewAggregatorList(ca, a)

func setup() {
	aDatas.Store("test-1", "tr1")
	aDatas.Store("test-2", "tr2")
	aDatas.Store("test-3", "tr3")

	// alist.Run()
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

func TestRun(t *testing.T) { // test -race -run TestRun
	go a.Run()
	go a.Run()
}

func TestAggregatorQuery(t *testing.T) {
	println(a.QueryValue("test-1"))
	println(a.QueryValue("test-2"))
	println(a.QueryValue("test-3"))
}

func TestAggregatorMulti(t *testing.T) {
	rs := a.QueryMulti([]string{"test-1", "test-2", "test-3"})
	for _, r := range rs {
		println("sync", r.Value)
	}

	var w sync.WaitGroup
	for i := 0; i < 2; i++ {
		w.Add(1)
		go func(i int) {
			defer w.Done()
			rs := a.QueryMulti([]string{"test-1", "test-2", "test-3"})
			for _, r := range rs {
				println(i, r.Value)
			}
		}(i)
	}
	w.Wait()
}

// split into 2 layer, cache and real
func TestQueryMulti(t *testing.T) {
	var w sync.WaitGroup

	w.Add(1)
	go func() {
		defer w.Done()
		rs := alist.QueryMulti([]string{"test-1"})
		for _, r := range rs {
			println("RESULT[1]", r.Value)
		}
	}()

	w.Add(1)
	go func() {
		defer w.Done()
		rs := alist.QueryMulti([]string{"test-1", "test-2", "test-3"})
		for _, r := range rs {
			println("RESULT[123]", r.Value)
		}
	}()

	w.Add(1)
	go func() {
		defer w.Done()
		rs := ca.QueryMulti([]string{"test-2", "test-3"})
		for _, r := range rs {
			println("RESULT[23]", r.Value)
		}
	}()

	w.Add(1)
	go func() {
		defer w.Done()
		time.Sleep(500 * time.Millisecond)
		rs := ca.QueryMulti([]string{"test-1"})
		for _, r := range rs {
			println("RESULT[1_sleep1]", r.Value)
		}
	}()

	w.Add(1)
	go func() {
		defer w.Done()
		time.Sleep(500 * time.Millisecond)
		rs := ca.QueryMulti([]string{"test-1"})
		for _, r := range rs {
			println("RESULT[1_sleep2]", r.Value)
		}
	}()

	w.Wait()
}
