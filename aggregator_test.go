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

func TestAggregatorSync(t *testing.T) {
	t.Parallel()

	db, progress := simpleProgress(0, 2)
	a := NewAggregator(progress, 10*time.Second, 1, 1)

	// test sync query
	assertEqual(t, a.QueryValue("key1"), "val1")
	assertEqual(t, a.QueryValue("key2"), "val2")
	assertEqual(t, a.Query("key3").Error, ErrNoResult)

	// insert data
	db.Store("key3", "val3")
	assertEqual(t, a.QueryValue("key3"), "val3")

	// Query
	assertEqual(t, a.Query("key1"), Result[string]{
		Value: "val1",
	})

	// QueryChan
	assertEqual(t, <-a.QueryChan("key1"), Result[string]{Value: "val1"})

	// QueryResult
	result, err := a.QueryResult("key1")
	assertEqual(t, result, "val1")
	assertEqual(t, err, nil)

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

func TestAggregatorAsync(t *testing.T) {
	t.Parallel()

	max := 3
	_, progress := simpleProgress(0, max)
	a := NewAggregator(func(ids []string) (map[string]string, error) {
		sort.Strings(ids)
		assertEqual(t, reflect.DeepEqual(ids, []string{"key1", "key2", "key3"}), true)
		return progress(ids)
	}, 100*time.Millisecond, 100, 1)

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

func TestAggregatorWorker(t *testing.T) {
	processTime := 90 * time.Millisecond
	delayTime := 33 * time.Millisecond
	tolerance := 10 * time.Millisecond
	runTest := func(t *testing.T, a *Aggregator[string, string], expectDurations map[string]time.Duration) {
		var w sync.WaitGroup
		w.Add(4)
		now := time.Now()
		for i := 1; i <= 4; i++ {
			go func(idx int) {
				defer w.Done()
				key := fmt.Sprintf("key%d", idx)
				assertEqual(t, a.QueryValue(key), fmt.Sprintf("val%d", idx))

				expectDuration := expectDurations[key]
				rt := time.Since(now)
				if diff := rt - expectDuration; diff < 0 || diff > tolerance {
					t.Error("idx", idx, "unexpected running duration (ms):", rt.Milliseconds(), "expected:", expectDuration.Milliseconds())
				}
			}(i)
			time.Sleep(delayTime)
		}
		w.Wait()
	}

	_, progress := simpleProgress(processTime, 4)

	assertEqual(t, canTestConcurrent(1), true)
	t.Run("1 worker", func(t *testing.T) {
		t.Parallel()

		a := NewAggregator(progress, 10*time.Second, 1, 1)
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

			a := NewAggregator(progress, 10*time.Second, 1, 2)
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

			a := NewAggregator(progress, 10*time.Second, 1, 4)
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

func canTestConcurrent(concurrent int) bool {
	return runtime.GOMAXPROCS(0) >= concurrent && runtime.NumCPU() >= concurrent
}

func simpleProgress(fakeDuration time.Duration, dbItemsCount int) (sync.Map, func(ids []string) (map[string]string, error)) {
	// generate sample db
	var db sync.Map
	for i := 1; i <= dbItemsCount; i++ {
		db.Store(fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i))
	}

	// generate progress
	progress := func(ids []string) (map[string]string, error) {
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

	return db, progress
}
