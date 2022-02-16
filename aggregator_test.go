package aggregator

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func assertEqual[T comparable](t *testing.T, a T, b T) {
	if a != b {
		t.Error("assert not equal:", a, b)
	}
}

func TestMain(m *testing.M) {
	// setup()
	code := m.Run()
	os.Exit(code)
}

// func TestRun(t *testing.T) { // test -race -run TestRun
// 	go a.Run()
// 	go a.Run()
// }

func TestAggregator(t *testing.T) {
	var db sync.Map
	db.Store("key1", "val1")
	db.Store("key2", "val2")

	a := NewAggregator(func(ids []string, args ...any) (map[string]string, error) {
		fmt.Printf("a process: %v", ids)
		out := make(map[string]string)
		for _, id := range ids {
			if r, ok := db.Load(id); ok {
				out[id] = r.(string)
			}
		}
		return out, nil
	}, 100*time.Millisecond, 100)
	a.Run()

	// test sync query
	assertEqual(t, a.QueryValue("key1"), "val1")
	assertEqual(t, a.QueryValue("key2"), "val2")
	assertEqual(t, a.Query("key3").Error, ErrNoResult)

	// test insert data
	db.Store("key3", "val3")
	assertEqual(t, a.QueryValue("key3"), "val3")
}
