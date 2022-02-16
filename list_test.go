package aggregator

import (
	"sync"
	"testing"
	"time"
)

func TestMultiAggregator(t *testing.T) {
	var w sync.WaitGroup
	i := 0
	for {
		w.Add(1)
		k := "test-1" //fmt.Sprintf("test-%d", i)
		go func(k string) {
			defer w.Done()
			println("got", k, alist.QueryValue(k))
		}(k)

		w.Add(1)
		go func(k string) {
			defer w.Done()
			println("got2", k, alist.QueryValue(k))
		}(k)

		i++
		// time.Sleep(200 * time.Millisecond)
		if i == 10 {
			break
		}
	}
	w.Wait()
}

func TestAggregatorUpdateCache(t *testing.T) {
	a.Timeout = 2000 * time.Millisecond
	a.Args = []interface{}{3000 * time.Millisecond} // sleep

	ca.Timeout = 10 * time.Millisecond
	ca.Args = []interface{}{1000 * time.Millisecond} // sleep

	var w sync.WaitGroup

	w.Add(1)
	go func() {
		defer w.Done()

		// old cache
		caDatas.Store("test-1", "tr1")

		go func() {
			rs := ca.QueryValue("test-1") // cache 10 + 1000 -> db 2000 + load 3000 = 6010
			println("EARLY RESULT", rs)
		}()

		time.Sleep(1500 * time.Millisecond)

		// update and clear cache
		aDatas.Store("test-1", "tr1mod")
		caDatas.Delete("test-1")

		// [bug] get old cache
		// {
		// 	rsr := ca.SfQuery("test-1")
		// 	println("RESULT", rsr.(string))
		// 	Equal(t, rsr.(string), "tr1mod")
		// }

		// solution 1: forget (but anthor worker will not work)
		// {
		// 	ca.SfQueryForget("test-1")
		// 	// [todo] need write back to cache, then return update result

		// 	// after update get new result
		// 	rsr := ca.SfQuery("test-1")
		// 	println("RESULT", rsr.(string))
		// 	Equal(t, rsr.(string), "tr1mod")
		// }

		// solution 2: read without Sf, need a flag to know after flag
		// deprecated: because dont want to return channel in aggregator
		// {
		// 	rsr := ca.Query("test-1")
		// 	println("RESULT", rsr.(string))
		// 	Equal(t, rsr.(string), "tr1mod")
		// }

		// solution 3: read outside
		{
			rsr := alist.QueryValue("test-1")
			println("RESULT", rsr)

			if rsr != "tr1mod" {
				t.Error("no equal")
			}
		}
	}()

	w.Wait()
}
