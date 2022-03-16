package aggregator

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

func assertEqual[T comparable](t *testing.T, a T, b T) {
	if a != b {
		t.Error("assert not equal:", a, b)
	}
}

func assertDuration(t *testing.T, duration time.Duration, want time.Duration) {
	tolerance := 10 * time.Millisecond
	if diff := duration - want; diff < 0 || diff > tolerance {
		t.Error("assert duration:", duration.Milliseconds(), "want:", want.Milliseconds())
	}
}

func TestSync(t *testing.T) {
	t.Parallel()

	db, fn := setupTestData(0, 2)
	a, _ := New(fn, 10*time.Second, 1).Run()

	// test sync query
	assertEqual(t, a.QueryValue("key1"), "val1")
	assertEqual(t, a.QueryValue("key2"), "val2")
	assertEqual(t, a.Query("key3").Error == NoResult, true)

	// insert data
	db.Store("key3", "val3")
	assertEqual(t, a.QueryValue("key3"), "val3")

	// Query
	assertEqual(t, a.Query("key1") == Result[string]{Value: "val1"}, true)

	// QueryChan
	assertEqual(t, <-a.QueryChan("key1") == Result[string]{Value: "val1"}, true)

	// QueryResult
	result, err := a.QueryResult("key1")
	assertEqual(t, result, "val1")
	assertEqual(t, err == nil, true)

	// QueryMulti
	results := a.QueryMulti([]string{"key1", "key2"})
	for i, result := range results { // avoid using reflect.DeepEqual with errors
		switch i {
		case 0:
			assertEqual(t, result.Value, "val1")
		case 1:
			assertEqual(t, result.Value, "val2")
		}
	}
}

func TestAsync(t *testing.T) {
	t.Parallel()

	max := 3
	_, fn := setupTestData(0, max)
	a, _ := New(func(ids []string) (map[string]string, error) {
		sort.Strings(ids)
		assertEqual(t, reflect.DeepEqual(ids, []string{"key1", "key2", "key3"}), true)
		return fn(ids)
	}, 100*time.Millisecond, 100).Run()

	var w sync.WaitGroup
	w.Add(max)
	for i := 1; i <= max; i++ {
		go func(idx int) {
			defer w.Done()
			assertEqual(t, a.QueryValue(fmt.Sprintf("key%d", idx)), fmt.Sprintf("val%d", idx))
		}(i)
	}
	w.Wait()
}

func TestWorker(t *testing.T) {
	processTime := 90 * time.Millisecond
	delayTime := 33 * time.Millisecond

	runTest := func(t *testing.T, a *Aggregator[string, string], wants map[string]time.Duration) {
		var w sync.WaitGroup
		w.Add(4)
		now := time.Now()
		for i := 1; i <= 4; i++ {
			go func(idx int) {
				defer w.Done()
				key := fmt.Sprintf("key%d", idx)
				assertEqual(t, a.QueryValue(key), fmt.Sprintf("val%d", idx))
				assertDuration(t, time.Since(now), wants[key])
			}(i)
			time.Sleep(delayTime)
		}
		w.Wait()
	}

	_, fn := setupTestData(processTime, 4)

	assertEqual(t, canTestConcurrent(1), true)
	t.Run("1 worker", func(t *testing.T) {
		t.Parallel()

		a, _ := New(fn, 10*time.Second, 1).Run()
		runTest(t, a, map[string]time.Duration{
			"key1": processTime,
			"key2": processTime * 2,
			"key3": processTime * 3,
			"key4": processTime * 4,
		})
	})

	if canTestConcurrent(2) {
		t.Run("2 workers", func(t *testing.T) {
			t.Parallel()

			a, _ := New(fn, 10*time.Second, 1).RunWithWorkers(2)
			runTest(t, a, map[string]time.Duration{
				"key1": processTime,
				"key2": processTime + delayTime,
				"key3": (processTime * 2),
				"key4": (processTime * 2) + delayTime,
			})
		})
	} else {
		t.Log("not enough procs to test 2 workers")
	}

	if canTestConcurrent(4) {
		t.Run("4 workers", func(t *testing.T) {
			t.Parallel()

			a, _ := New(fn, 10*time.Second, 1).RunWithWorkers(4)
			runTest(t, a, map[string]time.Duration{
				"key1": processTime,
				"key2": processTime + delayTime,
				"key3": processTime + delayTime*2,
				"key4": processTime + delayTime*3,
			})
		})
	} else {
		t.Log("not enough procs to test 4 workers")
	}
}

func TestUnlimitWait(t *testing.T) {
	t.Run("wait forever, never reach max query", func(t *testing.T) {
		t.Parallel()
		max := 2
		_, fn := setupTestData(0, max)
		a, _ := New(fn, NeverFlushTimeout, 3).Run()
		var w sync.WaitGroup
		w.Add(max)
		for i := 1; i <= max; i++ {
			go func(idx int) {
				defer w.Done()
				select {
				case <-a.QueryChan(fmt.Sprintf("key%d", idx)):
					t.Error("expect not return value")
				case <-time.After(100 * time.Millisecond):
					break
				}
			}(i)
		}
		w.Wait()
	})

	t.Run("wait forever, reach max query", func(t *testing.T) {
		t.Parallel()
		max := 2
		_, fn := setupTestData(0, max)
		a, _ := New(fn, NeverFlushTimeout, 2).Run()
		var w sync.WaitGroup
		w.Add(max)
		for i := 1; i <= max; i++ {
			go func(idx int) {
				defer w.Done()
				select {
				case v := <-a.QueryChan(fmt.Sprintf("key%d", idx)):
					assertEqual(t, v.Value, fmt.Sprintf("val%d", idx))
					break
				case <-time.After(100 * time.Millisecond):
					t.Error("expect flush by max query")
				}
			}(i)
		}
		w.Wait()
	})
}

func TestUnlimitMax(t *testing.T) {
	t.Parallel()

	max := 10
	flushTimeout := 500 * time.Millisecond
	_, fn := setupTestData(0, max)
	a, _ := New(fn, flushTimeout, 0).Run()

	var w sync.WaitGroup
	w.Add(max)
	now := time.Now()
	for i := 1; i <= max; i++ {
		go func(idx int) {
			defer w.Done()
			assertEqual(t, a.QueryValue(fmt.Sprintf("key%d", idx)), fmt.Sprintf("val%d", idx))
			assertDuration(t, time.Since(now), flushTimeout)
		}(i)
	}
	w.Wait()
}

func canTestConcurrent(concurrent int) bool {
	return runtime.GOMAXPROCS(0) >= concurrent && runtime.NumCPU() >= concurrent
}

func setupTestData(fakeDuration time.Duration, dbItemsCount int) (sync.Map, func(ids []string) (map[string]string, error)) {
	// generate sample db
	var db sync.Map
	for i := 1; i <= dbItemsCount; i++ {
		db.Store(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}

	// generate fn
	fn := func(ids []string) (map[string]string, error) {
		if fakeDuration > 0 {
			time.Sleep(fakeDuration)
		}
		out := make(map[string]string)
		for _, id := range ids {
			if r, ok := db.Load(id); ok {
				out[id] = r.(string)
			}
		}
		return out, nil
	}
	return db, fn
}
