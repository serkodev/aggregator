package aggregator

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type Aggregator[K comparable, T any] struct {
	Processor     func([]K) (map[K]T, error)
	FlushTimeout  time.Duration
	FlushMaxQuery int
	WorkerSize    int

	flushChan  chan map[K][]chan Result[T]
	notifyChan chan notifyObject[K, T]
	debugPrint func(format string, v ...interface{})
	runOnce    sync.Once
}

type notifyObject[K comparable, T any] struct {
	key K
	ch  chan Result[T]
}

// flushMaxQuery: 0 unlimit
// flushTimeout: only wait max query once if < 0
func NewAggregator[K comparable, T any](processor func([]K) (map[K]T, error), flushTimeout time.Duration, flushMaxQuery int) *Aggregator[K, T] {
	a := &Aggregator[K, T]{
		Processor:     processor,
		FlushTimeout:  flushTimeout,
		FlushMaxQuery: flushMaxQuery,
		WorkerSize:    1,

		notifyChan: make(chan notifyObject[K, T]),
		debugPrint: func(format string, v ...interface{}) {},
	}
	return a
}

func (a *Aggregator[K, T]) Debug(debug bool) {
	if debug {
		a.debugPrint = log.New(os.Stdout, fmt.Sprintf("[aggregator]"), 0).Printf
	} else {
		a.debugPrint = func(format string, v ...interface{}) {}
	}
}

func (a *Aggregator[K, T]) Run() *Aggregator[K, T] {
	a.runOnce.Do(func() {
		go a.run()
	})
	return a
}

func (a *Aggregator[K, T]) run() {
	// flush workers
	if a.WorkerSize < 1 {
		a.WorkerSize = 1
	}
	a.flushChan = make(chan map[K][]chan Result[T], a.WorkerSize)
	for i := 0; i < a.WorkerSize; i++ {
		go a.flushWorker(a.flushChan)
	}

	// flush timer
	t := time.NewTimer(a.FlushTimeout)
	for {
		// wait first notification
		data := <-a.notifyChan
		a.debugPrint("[query] key start: %s", data.key)
		fetchList := map[K][]chan Result[T]{
			data.key: {data.ch},
		}

		// clear timer
		if !t.Stop() && len(t.C) > 0 {
			<-t.C
		}

		if a.FlushMaxQuery > 1 { // TODO: a.MaxQueryOnce 0
			t.Reset(a.FlushTimeout)

			// wait other notification
		wait:
			for {
				select {
				case data := <-a.notifyChan:
					a.debugPrint("[query] key: %s", data.key)
					fetchList[data.key] = append(fetchList[data.key], data.ch)
					if a.FlushMaxQuery > 0 && len(fetchList) >= a.FlushMaxQuery {
						a.debugPrint("[flush] max query reached")
						break wait
					}
				case <-t.C:
					a.debugPrint("[flush] timeout")
					break wait
				}
			}
		}

		a.flushChan <- fetchList
	}
}

func (a *Aggregator[K, T]) flushWorker(fetchChan <-chan map[K][]chan Result[T]) {
	for fetchList := range fetchChan {
		keys := make([]K, len(fetchList))
		i := 0
		for k := range fetchList {
			keys[i] = k
			i++
		}

		// execute processor
		results, err := a.Processor(keys)

		// return results
		for i, queue := range fetchList {
			var result Result[T]

			if err != nil {
				result.Error = err
			} else if r, ok := results[i]; ok {
				result.Value = r
			} else {
				result.Error = ErrNoResult
			}

			// send result
			for _, c := range queue {
				go func(c chan Result[T]) {
					c <- result
					close(c)
				}(c)
			}
		}
	}
}

func (a *Aggregator[K, T]) QueryChan(key K) <-chan Result[T] {
	ch := make(chan Result[T], 1)
	a.notifyChan <- notifyObject[K, T]{
		key: key,
		ch:  ch,
	}
	return ch
}

func (a *Aggregator[K, T]) Query(key K) Result[T] {
	return <-a.QueryChan(key)
}

func (a *Aggregator[K, T]) QueryResult(key K) (T, error) {
	r := a.Query(key)
	return r.Value, r.Error
}

func (a *Aggregator[K, T]) QueryValue(key K) T {
	return a.Query(key).Value
}

func (a *Aggregator[K, T]) QueryMulti(keys []K) []Result[T] {
	output := make([]Result[T], len(keys))
	var w sync.WaitGroup
	w.Add(len(keys))
	for i, key := range keys {
		go func(i int, key K) {
			defer w.Done()
			output[i] = a.Query(key)
		}(i, key)
	}
	w.Wait()
	return output
}
